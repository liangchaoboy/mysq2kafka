package main

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestFilteringFollowsGroupRenameInSameTransaction(t *testing.T) {
	p := newProcessor("db2", []string{"供应商"}, map[string]bool{"100": true}, 100, []string{"qq_messages", "qq_groups", "qq_group_members"})
	pos := Position{File: "mysql-bin.000001", Pos: 120}
	p.handleRow("qq_messages", "insert", "a", nil, map[string]any{"id": 1, "group_number": "100"}, pos)
	p.handleRow("qq_messages", "insert", "b", nil, map[string]any{"id": 2, "group_number": "200"}, pos)
	p.handleRow("qq_messages", "update", "c", map[string]any{"id": 1, "group_number": "100"}, map[string]any{"id": 1, "group_number": "100"}, pos)
	p.handleRow("qq_groups", "update", "d", map[string]any{"qq": "100", "name": "供应商甲"}, map[string]any{"qq": "100", "name": "普通群"}, pos)
	p.handleRow("qq_group_members", "insert", "e", nil, map[string]any{"group_qq": "100", "member_info_qq_number": "9"}, pos)
	p.handleRow("qq_groups", "update", "f", map[string]any{"qq": "200", "name": "普通群"}, map[string]any{"qq": "200", "name": "供应商乙"}, pos)
	p.handleRow("qq_group_members", "insert", "g", nil, map[string]any{"group_qq": "200", "member_info_qq_number": "9"}, pos)
	if len(p.pending) != 4 {
		t.Fatalf("got %d events, want 4: %#v", len(p.pending), p.pending)
	}
	want := []string{"qq_messages/insert/100", "qq_groups/filter_exit/100", "qq_groups/update/200", "qq_group_members/insert/200"}
	for i, e := range p.pending {
		got := e.Table + "/" + e.Op + "/" + e.GroupQQ
		if got != want[i] {
			t.Errorf("event %d: got %q, want %q", i, got, want[i])
		}
	}
	p.applied()
	if p.groups["100"] || !p.groups["200"] {
		t.Fatalf("wrong group state: %#v", p.groups)
	}
}

func TestMemberKeyChangeEmitsDeleteAndInsert(t *testing.T) {
	p := newProcessor("db2", []string{"供应商"}, map[string]bool{"100": true, "200": true}, 100, []string{"qq_messages", "qq_groups", "qq_group_members"})
	pos := Position{File: "bin.1", Pos: 100}
	p.handleRow("qq_group_members", "update", "a", map[string]any{"group_qq": "100", "member_info_qq_number": "9"}, map[string]any{"group_qq": "200", "member_info_qq_number": "9"}, pos)
	if len(p.pending) != 2 || p.pending[0].Op != "delete" || p.pending[1].Op != "insert" {
		t.Fatalf("events: %#v", p.pending)
	}
	if p.pending[0].ID == p.pending[1].ID {
		t.Fatal("key-change events need distinct IDs")
	}
}

func TestRowsEventRejectsSchemaDriftAndHonorsDatabase(t *testing.T) {
	p := newProcessor("db2", []string{"供应商"}, nil, 100, []string{"qq_messages", "qq_groups", "qq_group_members"})
	wrongDB := &replication.RowsEvent{Table: &replication.TableMapEvent{Schema: []byte("other"), Table: []byte("qq_messages"), ColumnCount: 1}}
	if err := p.processRows("bin.1", 100, replication.WRITE_ROWS_EVENTv2, wrongDB); err != nil {
		t.Fatal(err)
	}
	wrongSchema := &replication.RowsEvent{Table: &replication.TableMapEvent{Schema: []byte("db2"), Table: []byte("qq_messages"), ColumnCount: 1}}
	if err := p.processRows("bin.1", 100, replication.WRITE_ROWS_EVENTv2, wrongSchema); err == nil {
		t.Fatal("expected schema drift error")
	}
}

func TestMessagesOnlyStillTracksNewAndRenamedSupplierGroups(t *testing.T) {
	p := newProcessor("db2", []string{"供应商"}, map[string]bool{}, 100, []string{"qq_messages"})
	pos := Position{File: "bin.1", Pos: 120}
	p.handleRow("qq_groups", "insert", "g1", nil, map[string]any{"qq": "100", "name": "甲供应商群"}, pos)
	p.handleRow("qq_messages", "insert", "m1", nil, map[string]any{"id": 1, "group_number": "100"}, pos)
	p.handleRow("qq_groups", "update", "g2", map[string]any{"qq": "200", "name": "普通群"}, map[string]any{"qq": "200", "name": "乙供应商群"}, pos)
	p.handleRow("qq_group_members", "insert", "member", nil, map[string]any{"group_qq": "200", "member_info_qq_number": "9"}, pos)
	p.handleRow("qq_messages", "insert", "m2", nil, map[string]any{"id": 2, "group_number": "200"}, pos)
	if len(p.pending) != 2 {
		t.Fatalf("got %d events, want two messages: %#v", len(p.pending), p.pending)
	}
	for _, event := range p.pending {
		if event.Table != "qq_messages" || event.Op != "insert" {
			t.Fatalf("unexpected event: %#v", event)
		}
	}
	if !p.isQualified("100") || !p.isQualified("200") {
		t.Fatal("new and renamed groups must affect message filtering")
	}
}

func TestMultipleGroupNameKeywordsMatchAny(t *testing.T) {
	p := newProcessor("db2", []string{"供应商", "供货商"}, map[string]bool{}, 100, []string{"qq_messages"})
	pos := Position{File: "bin.1", Pos: 120}
	for _, group := range []struct {
		qq   string
		name string
	}{
		{"100", "甲供应商群"},
		{"200", "乙供货商群"},
		{"300", "普通群"},
		{"400", "甲供应商及供货商群"},
	} {
		p.handleRow("qq_groups", "insert", "g"+group.qq, nil, map[string]any{"qq": group.qq, "name": group.name}, pos)
		p.handleRow("qq_messages", "insert", "m"+group.qq, nil, map[string]any{"group_number": group.qq}, pos)
	}
	if len(p.pending) != 3 {
		t.Fatalf("got %d messages, want 3: %#v", len(p.pending), p.pending)
	}
	for i, want := range []string{"100", "200", "400"} {
		if p.pending[i].GroupQQ != want {
			t.Errorf("message %d group = %q, want %q", i, p.pending[i].GroupQQ, want)
		}
	}
	p.handleRow("qq_groups", "update", "rename", map[string]any{"qq": "200", "name": "乙供货商群"}, map[string]any{"qq": "200", "name": "普通群"}, pos)
	p.handleRow("qq_messages", "insert", "after-rename", nil, map[string]any{"group_number": "200"}, pos)
	if len(p.pending) != 3 {
		t.Fatalf("renamed group should no longer match: %#v", p.pending)
	}
}
