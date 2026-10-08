package main

import (
	"path/filepath"
	"testing"
)

func TestCheckpointPersistsGroupStateAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	initial := Position{File: "bin.000001", Pos: 4}
	if err := s.bootstrap("mysql/db2", initial, map[string]bool{"100": true}); err != nil {
		t.Fatal(err)
	}
	next := Position{File: "bin.000002", Pos: 123}
	if err := s.commit(next, map[string]bool{"100": false, "200": true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	pos, groups, found, err := s.load("mysql/db2")
	if err != nil {
		t.Fatal(err)
	}
	if !found || pos != next || groups["100"] || !groups["200"] {
		t.Fatalf("pos=%+v groups=%#v found=%t", pos, groups, found)
	}
	if _, _, _, err := s.load("another-source"); err == nil {
		t.Fatal("source identity mismatch should fail")
	}
}
