package database

import (
	"reflect"
	"testing"
)

func TestGenerateAdvisoryLockId(t *testing.T) {
	id, err := GenerateAdvisoryLockId("database_name")
	if err != nil {
		t.Errorf("expected err to be nil, got %v", err)
	}
	if len(id) == 0 {
		t.Errorf("expected generated id not to be empty")
	}
	t.Logf("generated id: %v", id)
}

func TestSplitQuery(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"CREATE TABLE;ALTER TABLE;DROP TABLE", []string{"CREATE TABLE", "ALTER TABLE", "DROP TABLE"}},
		{"CREATE TABLE\n;\nALTER TABLE;\n;DROP TABLE;\n", []string{"CREATE TABLE\n", "ALTER TABLE", "", "DROP TABLE"}},
		{`COMMENT 'foo\'; "bar'; COMMENT "quote\";escape'"`, []string{`COMMENT 'foo\'; "bar'`, `COMMENT "quote\";escape'"`}},
	}

	for _, tt := range tests {
		got := SplitQuery([]byte(tt.input))
		strs := make([]string, len(got))
		for i, buf := range got {
			strs[i] = string(buf)
		}
		if !reflect.DeepEqual(tt.expected, strs) {
			t.Errorf("expected: %q, but got %q", tt.expected, strs)
		}
	}
}
