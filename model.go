package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

var columns = map[string][]string{
	"qq_messages":      {"id", "sender_number", "display_name", "nick_name", "content", "group_number", "send_time", "created_at", "checksum", "avatar", "timeline", "message_type", "local_id"},
	"qq_groups":        {"id", "qq", "name", "uid", "status", "group_type", "note_content", "chat_type", "created_at", "updated_at", "rank", "tag", "chat_id", "link_uids"},
	"qq_group_members": {"group_qq", "member_info_qq_number"},
}

type Position struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

type OutEvent struct {
	ID      string         `json:"event_id"`
	Table   string         `json:"table"`
	Op      string         `json:"op"`
	GroupQQ string         `json:"group_qq"`
	Data    map[string]any `json:"data"`
	Binlog  Position       `json:"binlog"`
}

func (e OutEvent) record() ([]byte, []byte, error) {
	v, err := json.Marshal(e)
	return []byte(e.GroupQQ), v, err
}

func rowObject(table string, row []any) (map[string]any, error) {
	names := columns[table]
	if len(row) != len(names) {
		return nil, fmt.Errorf("%s: binlog row has %d columns, expected %d; check schema and binlog_row_image=FULL", table, len(row), len(names))
	}
	m := make(map[string]any, len(names))
	for i, name := range names {
		v := row[i]
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
		m[name] = v
	}
	return m, nil
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}

func qualifies(name string, keywords []string) bool {
	for _, keyword := range keywords {
		if strings.Contains(name, keyword) {
			return true
		}
	}
	return false
}
