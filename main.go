package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	gosqlmysql "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	configPath := flag.String("config", "config.json", "configuration file")
	checkOnly := flag.Bool("check", false, "check MySQL schema and binlog without starting replication")
	flag.Parse()
	if err := run(*configPath, *checkOnly); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string, checkOnly bool) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	dsn := gosqlmysql.NewConfig()
	dsn.User, dsn.Passwd = c.MySQL.User, c.MySQL.Password
	dsn.Net = "tcp"
	dsn.Addr = fmt.Sprintf("%s:%d", c.MySQL.Host, c.MySQL.Port)
	dsn.DBName = c.MySQL.Database
	dsn.Timeout = 10 * time.Second
	dsn.ReadTimeout = 30 * time.Second
	dsn.WriteTimeout = 30 * time.Second
	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("mysql connection: %w", err)
	}
	if err := verifySource(ctx, db, c.MySQL.Database); err != nil {
		return err
	}
	if checkOnly {
		log.Print("MySQL connection, binlog settings and three table schemas: OK")
		if err := inspectGrants(ctx, db); err != nil {
			return err
		}
		matching, err := countMatchingGroups(ctx, db, c.GroupNameFilter)
		if err != nil {
			return err
		}
		log.Printf("currently matching groups: %d (no data published by --check)", matching)
		return nil
	}
	store, err := openStore(c.StateFile)
	if err != nil {
		return err
	}
	defer store.Close()
	identity := fmt.Sprintf("%s:%d/%s", c.MySQL.Host, c.MySQL.Port, c.MySQL.Database)
	pos, groups, exists, err := store.load(identity)
	if err != nil {
		return err
	}
	if !exists {
		pos, groups, err = bootstrap(ctx, db, store, identity, c)
		if err != nil {
			return err
		}
		log.Printf("initialized at %s:%d with %d matching groups; no historical rows sent", pos.File, pos.Pos, len(groups))
	} else {
		log.Printf("resuming at %s:%d with %d matching groups", pos.File, pos.Pos, len(groups))
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(c.Kafka.Brokers...), kgo.RecordDeliveryTimeout(30*time.Second))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: c.MySQL.ServerID, Flavor: "mysql", Host: c.MySQL.Host,
		Port: c.MySQL.Port, User: c.MySQL.User, Password: c.MySQL.Password,
		ParseTime: false, TimestampStringLocation: time.UTC,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(gomysql.Position{Name: pos.File, Pos: pos.Pos})
	if err != nil {
		return fmt.Errorf("start binlog at %s:%d: %w", pos.File, pos.Pos, err)
	}
	proc := newProcessor(c.MySQL.Database, c.GroupNameFilter, groups, c.MaxTransactionRows, c.PublishTables)
	file := pos.File
	for {
		event, err := streamer.GetEvent(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("read binlog: %w", err)
		}
		switch e := event.Event.(type) {
		case *replication.RotateEvent:
			file = string(e.NextLogName)
		case *replication.RowsEvent:
			if err := proc.processRows(file, event.Header.LogPos, event.Header.EventType, e); err != nil {
				return err
			}
		case *replication.XIDEvent:
			if err := flush(ctx, client, store, proc, c.Kafka.Topic, Position{File: file, Pos: event.Header.LogPos}); err != nil {
				return err
			}
		case *replication.QueryEvent:
			query := strings.ToUpper(strings.TrimSpace(string(e.Query)))
			if query == "BEGIN" {
				continue
			}
			if query == "ROLLBACK" {
				return errors.New("unexpected rollback in row binlog; restart from last checkpoint")
			}
			if isDDL(query) && (string(e.Schema) == c.MySQL.Database || string(e.Schema) == "") {
				return fmt.Errorf("DDL detected in watched database; verify table schemas and restart: %.160s", e.Query)
			}
			if query == "COMMIT" || proc.rows == 0 {
				if err := flush(ctx, client, store, proc, c.Kafka.Topic, Position{File: file, Pos: event.Header.LogPos}); err != nil {
					return err
				}
			}
		}
	}
}

func isDDL(query string) bool {
	for _, prefix := range []string{"ALTER ", "CREATE ", "DROP ", "RENAME ", "TRUNCATE "} {
		if strings.HasPrefix(query, prefix) {
			return true
		}
	}
	return false
}

func flush(ctx context.Context, client *kgo.Client, store *Store, proc *Processor, topic string, pos Position) error {
	for i := 0; i < len(proc.pending); i += 500 {
		end := i + 500
		if end > len(proc.pending) {
			end = len(proc.pending)
		}
		records := make([]*kgo.Record, 0, end-i)
		for _, event := range proc.pending[i:end] {
			key, value, err := event.record()
			if err != nil {
				return err
			}
			records = append(records, &kgo.Record{Topic: topic, Key: key, Value: value})
		}
		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			return fmt.Errorf("publish Kafka batch: %w", err)
		}
	}
	if err := store.commit(pos, proc.changes); err != nil {
		return fmt.Errorf("save checkpoint after Kafka publish: %w", err)
	}
	if len(proc.pending) > 0 {
		log.Printf("published %d events; checkpoint %s:%d", len(proc.pending), pos.File, pos.Pos)
	}
	proc.applied()
	return nil
}

func verifySource(ctx context.Context, db *sql.DB, database string) error {
	var logBin, format, image string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image").Scan(&logBin, &format, &image); err != nil {
		return err
	}
	if logBin != "1" && !strings.EqualFold(logBin, "ON") {
		return errors.New("MySQL binary logging is disabled")
	}
	if !strings.EqualFold(format, "ROW") || !strings.EqualFold(image, "FULL") {
		return fmt.Errorf("MySQL requires binlog_format=ROW and binlog_row_image=FULL; got %s/%s", format, image)
	}
	for table, expected := range columns {
		rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", database, table)
		if err != nil {
			return err
		}
		var actual []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			actual = append(actual, name)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(actual) != len(expected) {
			return fmt.Errorf("%s schema does not match supplied DDL: column count %d vs %d", table, len(actual), len(expected))
		}
		for i := range expected {
			if actual[i] != expected[i] {
				return fmt.Errorf("%s column %d is %q, expected %q", table, i+1, actual[i], expected[i])
			}
		}
	}
	return nil
}

func inspectGrants(ctx context.Context, db *sql.DB) error {
	var account, version string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER(), VERSION()").Scan(&account, &version); err != nil {
		return fmt.Errorf("check authenticated account: %w", err)
	}
	log.Printf("authenticated MySQL account: %s; version: %s", account, version)
	rows, err := db.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER")
	if err != nil {
		return fmt.Errorf("check current user grants: %w", err)
	}
	defer rows.Close()
	var grants strings.Builder
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return err
		}
		grants.WriteString(strings.ToUpper(grant))
		grants.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		return err
	}
	g := grants.String()
	all := strings.Contains(g, "ALL PRIVILEGES")
	replication := all || strings.Contains(g, "REPLICATION SLAVE") || strings.Contains(g, "REPLICATION REPLICA")
	lock := all || strings.Contains(g, "LOCK TABLES")
	log.Printf("current user grants: replication=%t, LOCK TABLES=%t (roles may grant additional privileges)", replication, lock)
	return nil
}

func countMatchingGroups(ctx context.Context, db *sql.DB, keywords []string) (int, error) {
	rows, err := db.QueryContext(ctx, "SELECT qq, name FROM qq_groups")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var qq, name sql.NullString
		if err := rows.Scan(&qq, &name); err != nil {
			return 0, err
		}
		if qq.Valid && qq.String != "" && name.Valid && qualifies(name.String, keywords) {
			count++
		}
	}
	return count, rows.Err()
}

func bootstrap(ctx context.Context, db *sql.DB, store *Store, identity string, c Config) (Position, map[string]bool, error) {
	lockConn, err := db.Conn(ctx)
	if err != nil {
		return Position{}, nil, err
	}
	defer lockConn.Close()
	lockQuery := "LOCK TABLES `" + c.MySQL.Database + "`.`qq_groups` READ"
	if _, err := lockConn.ExecContext(ctx, lockQuery); err != nil {
		return Position{}, nil, fmt.Errorf("bootstrap group lock requires LOCK TABLES privilege: %w", err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
		}
	}()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return Position{}, nil, err
	}
	defer tx.Rollback()
	query := "SELECT qq, name FROM `" + c.MySQL.Database + "`.`qq_groups`"
	groupRows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return Position{}, nil, err
	}
	defer groupRows.Close()
	rows, err := lockConn.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		modernErr := err
		rows, err = lockConn.QueryContext(ctx, "SHOW MASTER STATUS")
		if err != nil {
			return Position{}, nil, fmt.Errorf("binary log status: modern syntax: %v; legacy syntax: %w", modernErr, err)
		}
	}
	var pos Position
	if rows.Next() {
		colNames, err := rows.Columns()
		if err != nil {
			rows.Close()
			return Position{}, nil, err
		}
		values := make([]any, len(colNames))
		ptrs := make([]any, len(values))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return Position{}, nil, err
		}
		if len(values) < 2 {
			rows.Close()
			return Position{}, nil, errors.New("binary log status returned too few columns")
		}
		pos.File = str(values[0])
		if _, err := fmt.Sscan(str(values[1]), &pos.Pos); err != nil {
			rows.Close()
			return Position{}, nil, err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Position{}, nil, err
	}
	rows.Close()
	if pos.File == "" {
		return Position{}, nil, errors.New("binary log status returned no binlog position")
	}
	if _, err := lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		return Position{}, nil, err
	}
	locked = false
	groups := make(map[string]bool)
	for groupRows.Next() {
		var qq, name sql.NullString
		if err := groupRows.Scan(&qq, &name); err != nil {
			groupRows.Close()
			return Position{}, nil, err
		}
		if qq.Valid && qq.String != "" && name.Valid && qualifies(name.String, c.GroupNameFilter) {
			groups[qq.String] = true
		}
	}
	if err := groupRows.Err(); err != nil {
		groupRows.Close()
		return Position{}, nil, err
	}
	groupRows.Close()
	if err := tx.Commit(); err != nil {
		return Position{}, nil, err
	}
	if err := store.bootstrap(identity, pos, groups); err != nil {
		return Position{}, nil, err
	}
	return pos, groups, nil
}
