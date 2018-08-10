// https://github.com/mattes/migrate/blob/035c07716cd373d88456ec4d701402df52584cb4/database/mysql/mysql.go
// を元に機能拡張
// - 過去の migration 実行履歴を保存するようにする
//
// 変更箇所:
// - SetVersion, Version メソッド実装変更
// - FindVersion, DeleteVersion メソッド追加

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strconv"
	"strings"

	"github.com/C-FO/migrate"
	"github.com/C-FO/migrate/database"
	"github.com/go-sql-driver/mysql"
)

func init() {
	database.Register("mysql", &Mysql{})
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrAppendPEM      = fmt.Errorf("failed to append PEM")
)

type Config struct {
	MigrationsTable string
	DatabaseName    string
}

type Mysql struct {
	db       *sql.DB
	isLocked bool

	config *Config
}

// instance must have `multiStatements` set to true
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	query := `SELECT DATABASE()`
	var databaseName sql.NullString
	if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(databaseName.String) == 0 {
		return nil, ErrNoDatabaseName
	}

	config.DatabaseName = databaseName.String

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	mx := &Mysql{
		db:     instance,
		config: config,
	}

	if err := mx.ensureVersionTable(); err != nil {
		return nil, err
	}

	return mx, nil
}

func (m *Mysql) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	q := purl.Query()
	q.Set("multiStatements", "true")
	purl.RawQuery = q.Encode()

	db, err := sql.Open("mysql", strings.Replace(
		migrate.FilterCustomQuery(purl).String(), "mysql://", "", 1))
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	if len(migrationsTable) == 0 {
		migrationsTable = DefaultMigrationsTable
	}

	// use custom TLS?
	ctls := purl.Query().Get("tls")
	if len(ctls) > 0 {
		if _, isBool := readBool(ctls); !isBool && strings.ToLower(ctls) != "skip-verify" {
			rootCertPool := x509.NewCertPool()
			pem, err := ioutil.ReadFile(purl.Query().Get("x-tls-ca"))
			if err != nil {
				return nil, err
			}

			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				return nil, ErrAppendPEM
			}

			certs, err := tls.LoadX509KeyPair(purl.Query().Get("x-tls-cert"), purl.Query().Get("x-tls-key"))
			if err != nil {
				return nil, err
			}

			insecureSkipVerify := false
			if len(purl.Query().Get("x-tls-insecure-skip-verify")) > 0 {
				x, err := strconv.ParseBool(purl.Query().Get("x-tls-insecure-skip-verify"))
				if err != nil {
					return nil, err
				}
				insecureSkipVerify = x
			}

			mysql.RegisterTLSConfig(ctls, &tls.Config{
				RootCAs:            rootCertPool,
				Certificates:       []tls.Certificate{certs},
				InsecureSkipVerify: insecureSkipVerify,
			})
		}
	}

	mx, err := WithInstance(db, &Config{
		DatabaseName:    purl.Path,
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return nil, err
	}

	return mx, nil
}

func (m *Mysql) Close() error {
	return m.db.Close()
}

func (m *Mysql) Lock() error {
	if m.isLocked {
		return database.ErrLocked
	}

	aid, err := database.GenerateAdvisoryLockId(m.config.DatabaseName)
	if err != nil {
		return err
	}

	query := "SELECT GET_LOCK(?, 1)"
	var success bool
	if err := m.db.QueryRow(query, aid).Scan(&success); err != nil {
		return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
	}

	if success {
		m.isLocked = true
		return nil
	}

	return database.ErrLocked
}

func (m *Mysql) Unlock() error {
	if !m.isLocked {
		return nil
	}

	aid, err := database.GenerateAdvisoryLockId(m.config.DatabaseName)
	if err != nil {
		return err
	}

	query := `SELECT RELEASE_LOCK(?)`
	if _, err := m.db.Exec(query, aid); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	m.isLocked = false
	return nil
}

func (m *Mysql) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	query := string(migr[:])
	if _, err := m.db.Exec(query); err != nil {
		return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	}

	return nil
}

func (m *Mysql) SetVersion(version int, dirty bool) error {
	tx, err := m.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	// mattes/migrate の元実装では最後の version の情報しか持たないためここで TRUNCATE するが、
	// 本実装では version ごとに履歴を持つため TRUNCATE せず INSERT or UPDATE する

	if version >= 0 {
		fVersion, _, _ := m.FindVersion(version)
		// 該当 version の行が存在すれば UPDATE する
		if fVersion >= 0 {
			query := "UPDATE `" + m.config.MigrationsTable + "` SET dirty = ? WHERE version = ?"
			if _, err := m.db.Exec(query, dirty, version); err != nil {
				tx.Rollback()
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		} else { // 該当 version の行が存在しなければ INSERT する
			query := "INSERT INTO `" + m.config.MigrationsTable + "` (version, dirty) VALUES (?, ?)"
			if _, err := m.db.Exec(query, version, dirty); err != nil {
				tx.Rollback()
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (m *Mysql) Version() (version int, dirty bool, err error) {
	// 本実装では最も version が大きいものを返す
	query := "SELECT version, dirty FROM `" + m.config.MigrationsTable + "` ORDER BY version DESC LIMIT 1"
	err = m.db.QueryRow(query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*mysql.MySQLError); ok {
			if e.Number == 0 {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

// FindVersion 指定 version の履歴を取得する
func (m *Mysql) FindVersion(optVersion int) (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM `" + m.config.MigrationsTable + "` WHERE version = ? LIMIT 1"
	err = m.db.QueryRow(query, optVersion).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, err

	case err != nil:
		return database.NilVersion, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

// DeleteVersion 指定 version の履歴を削除する
func (m *Mysql) DeleteVersion(version int) error {
	query := "DELETE FROM `" + m.config.MigrationsTable + "` WHERE version = ?"
	if _, err := m.db.Exec(query, version); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (m *Mysql) Drop() error {
	// select all tables
	query := `SHOW TABLES LIKE '%'`
	tables, err := m.db.Query(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer tables.Close()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query = "DROP TABLE IF EXISTS `" + t + "` CASCADE"
			if _, err := m.db.Exec(query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
		if err := m.ensureVersionTable(); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mysql) ensureVersionTable() error {
	// check if migration table exists
	var result string
	query := `SHOW TABLES LIKE "` + m.config.MigrationsTable + `"`
	if err := m.db.QueryRow(query).Scan(&result); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	query = "CREATE TABLE `" + m.config.MigrationsTable + "` (version bigint not null primary key, dirty boolean not null)"
	if _, err := m.db.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
// See https://github.com/go-sql-driver/mysql/blob/a059889267dc7170331388008528b3b44479bffb/utils.go#L71
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}
