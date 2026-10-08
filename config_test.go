package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoadConfigAcceptsMultipleKeywordsAndLegacyString(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		want   NameFilter
	}{
		{"multiple keywords", `["供应商", "供货商"]`, NameFilter{"供应商", "供货商"}},
		{"legacy string", `"供应商"`, NameFilter{"供应商"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := loadConfigWithFilter(t, tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.GroupNameFilter, tt.want) {
				t.Fatalf("group_name_filter = %#v, want %#v", c.GroupNameFilter, tt.want)
			}
		})
	}
}

func TestLoadConfigRejectsEmptyKeywords(t *testing.T) {
	for _, filter := range []string{`[]`, `""`, `["供应商", ""]`, `[" "]`, `null`} {
		t.Run(filter, func(t *testing.T) {
			if _, err := loadConfigWithFilter(t, filter); err == nil {
				t.Fatal("expected invalid group_name_filter to be rejected")
			}
		})
	}
}

func loadConfigWithFilter(t *testing.T, filter string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	data := fmt.Sprintf(`{
		"mysql": {"host": "localhost", "user": "test", "database": "db2", "server_id": 1},
		"kafka": {"brokers": ["localhost:9092"], "topic": "events"},
		"state_file": "state.db", "group_name_filter": %s
	}`, filter)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return loadConfig(path)
}
