package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var bucketMeta = []byte("meta")
var bucketGroups = []byte("qualified_groups")
var keyPosition = []byte("position")
var keyIdentity = []byte("identity")

type Store struct{ db *bolt.DB }

func openStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketGroups)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) load(identity string) (Position, map[string]bool, bool, error) {
	var pos Position
	groups := make(map[string]bool)
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		storedIdentity := meta.Get(keyIdentity)
		if storedIdentity != nil && string(storedIdentity) != identity {
			return fmt.Errorf("state belongs to %q, current source is %q; use a separate state file", storedIdentity, identity)
		}
		if raw := meta.Get(keyPosition); raw != nil {
			found = true
			if err := json.Unmarshal(raw, &pos); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketGroups).ForEach(func(k, _ []byte) error { groups[string(k)] = true; return nil })
	})
	if err != nil {
		return Position{}, nil, false, err
	}
	return pos, groups, found, nil
}

func (s *Store) bootstrap(identity string, pos Position, groups map[string]bool) error {
	if pos.File == "" || pos.Pos < 4 {
		return errors.New("invalid bootstrap binlog position")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if meta.Get(keyPosition) != nil {
			return errors.New("state already initialized")
		}
		for group := range groups {
			if err := tx.Bucket(bucketGroups).Put([]byte(group), []byte{1}); err != nil {
				return err
			}
		}
		raw, err := json.Marshal(pos)
		if err != nil {
			return err
		}
		if err := meta.Put(keyIdentity, []byte(identity)); err != nil {
			return err
		}
		return meta.Put(keyPosition, raw)
	})
}

func (s *Store) commit(pos Position, changes map[string]bool) error {
	if pos.File == "" || pos.Pos < 4 {
		return errors.New("invalid checkpoint position")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketGroups)
		for group, qualified := range changes {
			var err error
			if qualified {
				err = b.Put([]byte(group), []byte{1})
			} else {
				err = b.Delete([]byte(group))
			}
			if err != nil {
				return err
			}
		}
		raw, err := json.Marshal(pos)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put(keyPosition, raw)
	})
}
