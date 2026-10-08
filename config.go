package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	MySQL struct {
		Host     string `json:"host"`
		Port     uint16 `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
		ServerID uint32 `json:"server_id"`
	} `json:"mysql"`
	Kafka struct {
		Brokers []string `json:"brokers"`
		Topic   string   `json:"topic"`
	} `json:"kafka"`
	StateFile          string     `json:"state_file"`
	GroupNameFilter    NameFilter `json:"group_name_filter"`
	PublishTables      []string   `json:"publish_tables"`
	MaxTransactionRows int        `json:"max_transaction_rows"`
}

// NameFilter accepts both the current keyword array and legacy single strings.
type NameFilter []string

func (f *NameFilter) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*f = NameFilter{single}
		return nil
	}
	var multiple []string
	if err := json.Unmarshal(data, &multiple); err != nil {
		return fmt.Errorf("group_name_filter must be a string or an array of strings: %w", err)
	}
	*f = multiple
	return nil
}

func loadConfig(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("decode config: %w", err)
	}
	if c.MySQL.Port == 0 {
		c.MySQL.Port = 3306
	}
	if c.MaxTransactionRows == 0 {
		c.MaxTransactionRows = 100000
	}
	if c.MySQL.Host == "" || c.MySQL.User == "" || c.MySQL.Database == "" || c.MySQL.ServerID == 0 ||
		len(c.Kafka.Brokers) == 0 || c.Kafka.Topic == "" || c.StateFile == "" || len(c.GroupNameFilter) == 0 {
		return c, errors.New("host, user, database, server_id, brokers, topic, state_file and group_name_filter are required")
	}
	for _, keyword := range c.GroupNameFilter {
		if strings.TrimSpace(keyword) == "" {
			return c, errors.New("group_name_filter keywords must be non-empty")
		}
	}
	if c.MaxTransactionRows < 1 {
		return c, errors.New("max_transaction_rows must be positive")
	}
	if len(c.PublishTables) == 0 {
		c.PublishTables = []string{"qq_messages", "qq_groups", "qq_group_members"}
	}
	seen := make(map[string]bool, len(c.PublishTables))
	for _, table := range c.PublishTables {
		if _, ok := columns[table]; !ok {
			return c, fmt.Errorf("unsupported publish_tables entry %q", table)
		}
		if seen[table] {
			return c, fmt.Errorf("duplicate publish_tables entry %q", table)
		}
		seen[table] = true
	}
	if strings.ContainsAny(c.MySQL.Database, "\x00`\n\r") {
		return c, errors.New("invalid database name")
	}
	return c, nil
}
