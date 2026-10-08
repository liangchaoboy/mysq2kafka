package main

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

type Processor struct {
	database string
	keywords []string
	groups   map[string]bool
	changes  map[string]bool
	pending  []OutEvent
	maxRows  int
	rows     int
	publish  map[string]bool
}

func newProcessor(database string, keywords []string, groups map[string]bool, maxRows int, publishTables []string) *Processor {
	publish := make(map[string]bool, len(publishTables))
	for _, table := range publishTables {
		publish[table] = true
	}
	return &Processor{database: database, keywords: keywords, groups: groups, changes: make(map[string]bool), maxRows: maxRows, publish: publish}
}

func (p *Processor) isQualified(group string) bool {
	if v, ok := p.changes[group]; ok {
		return v
	}
	return p.groups[group]
}

func (p *Processor) setQualified(group string, value bool) {
	if group != "" {
		p.changes[group] = value
	}
}

func (p *Processor) appendEvent(base string, n int, table, op, group string, data map[string]any, pos Position) {
	if !p.publish[table] {
		return
	}
	p.pending = append(p.pending, OutEvent{
		ID: fmt.Sprintf("%s/%d", base, n), Table: table, Op: op,
		GroupQQ: group, Data: data, Binlog: pos,
	})
}

func (p *Processor) handleRow(table, op, base string, before, after map[string]any, pos Position) {
	switch table {
	case "qq_messages":
		if op == "insert" {
			group := str(after["group_number"])
			if p.isQualified(group) {
				p.appendEvent(base, 0, table, op, group, after, pos)
			}
		}
	case "qq_groups":
		oldGroup := str(before["qq"])
		newGroup := str(after["qq"])
		oldQualified := oldGroup != "" && p.isQualified(oldGroup)
		newQualified := after != nil && newGroup != "" && qualifies(str(after["name"]), p.keywords)
		if oldGroup != "" && oldGroup != newGroup {
			p.setQualified(oldGroup, false)
		}
		if newGroup != "" {
			p.setQualified(newGroup, newQualified)
		}
		if oldQualified && (op == "delete" || oldGroup != newGroup || !newQualified) {
			eventOp := "filter_exit"
			if op == "delete" {
				eventOp = "delete"
			}
			p.appendEvent(base, 0, table, eventOp, oldGroup, before, pos)
		}
		if newQualified {
			idx := 0
			if oldQualified && oldGroup != newGroup {
				idx = 1
			}
			p.appendEvent(base, idx, table, op, newGroup, after, pos)
		}
	case "qq_group_members":
		oldGroup := str(before["group_qq"])
		newGroup := str(after["group_qq"])
		oldMember := str(before["member_info_qq_number"])
		newMember := str(after["member_info_qq_number"])
		keyChanged := oldGroup != newGroup || oldMember != newMember
		oldQualified := before != nil && p.isQualified(oldGroup)
		newQualified := after != nil && p.isQualified(newGroup)
		if oldQualified && (op == "delete" || keyChanged) {
			p.appendEvent(base, 0, table, "delete", oldGroup, before, pos)
		}
		if newQualified {
			eventOp := op
			idx := 0
			if keyChanged {
				eventOp = "insert"
				idx = 1
			}
			p.appendEvent(base, idx, table, eventOp, newGroup, after, pos)
		}
	}
}

func (p *Processor) processRows(file string, endPos uint32, kind replication.EventType, ev *replication.RowsEvent) error {
	if ev.Table == nil || string(ev.Table.Schema) != p.database {
		return nil
	}
	table := string(ev.Table.Table)
	if _, ok := columns[table]; !ok {
		return nil
	}
	if table != "qq_groups" && !p.publish[table] {
		return nil
	}
	if int(ev.Table.ColumnCount) != len(columns[table]) {
		return fmt.Errorf("%s schema column count changed; stop and update the tool", table)
	}
	var op string
	switch kind {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		op = "insert"
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		op = "update"
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		op = "delete"
	default:
		return fmt.Errorf("unsupported row event type %s for %s", kind, table)
	}
	step := 1
	if op == "update" {
		step = 2
		if len(ev.Rows)%2 != 0 {
			return fmt.Errorf("odd update rows for %s", table)
		}
	}
	for i := 0; i < len(ev.Rows); i += step {
		p.rows++
		if p.rows > p.maxRows {
			return fmt.Errorf("transaction exceeds max_transaction_rows=%d", p.maxRows)
		}
		var before, after map[string]any
		var err error
		if op != "insert" {
			before, err = rowObject(table, ev.Rows[i])
			if err != nil {
				return err
			}
		}
		if op == "insert" {
			after, err = rowObject(table, ev.Rows[i])
		} else if op == "update" {
			after, err = rowObject(table, ev.Rows[i+1])
		}
		if err != nil {
			return err
		}
		base := fmt.Sprintf("%s:%d:%d", file, endPos, i)
		p.handleRow(table, op, base, before, after, Position{File: file, Pos: endPos})
	}
	return nil
}

func (p *Processor) applied() {
	for group, value := range p.changes {
		if value {
			p.groups[group] = true
		} else {
			delete(p.groups, group)
		}
	}
	p.changes = make(map[string]bool)
	p.pending = nil
	p.rows = 0
}
