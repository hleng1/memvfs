package memvfs_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/hleng1/memvfs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

var v *memvfs.MemVFS

func TestMain(m *testing.M) {
	v = memvfs.New()

	if err := sqlite3vfs.RegisterVFS("memvfs", v); err != nil {
		log.Fatalf("Failed to register VFS: %v", err)
	}

	code := m.Run()

	os.Exit(code)
}

func TestMemVFS(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:test.db?vfs=memvfs&cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS demo (
			id INTEGER PRIMARY KEY,
			data TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO demo(data) VALUES ('Hello world from memvfs')`)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	var id int
	var data string
	row := db.QueryRowContext(ctx, `SELECT id, data FROM demo LIMIT 1`)
	if err := row.Scan(&id, &data); err != nil {
		t.Fatalf("Select error: %v", err)
	}
	t.Logf("Got row: id=%d data=%q\n", id, data)

	_, err = v.Access("test.db", sqlite3vfs.AccessExists)
	if err != nil {
		t.Logf("Failed to access %v: %v\n", "test.db", err)
	}

	// TODO more tests like https://github.com/psanford/donutdb/blob/main/donutdb_test.go

	db.Close()

	_, err = db.ExecContext(ctx, `INSERT INTO demo(data) VALUES ('Hello again from memvfs')`)
	if err == nil {
		t.Fatalf("%v should not allow insertion after Close: %v", "test.db", err)
	}

	_, err = v.Access("test.db", sqlite3vfs.AccessExists)
	if err == nil {
		t.Fatalf("%v is still accessible after Close: %v", "test.db", err)
	}
}

func TestConcurrentInsert(t *testing.T) {
	const (
		goroutineCount = 10
		iterations     = 100
	)

	db, err := sql.Open("sqlite3", "file:test_concurrent.db?&vfs=memvfs&cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS test (
            id INTEGER PRIMARY KEY,
            value TEXT
        )
    `)
	if err != nil {
		t.Fatalf("Create table error: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(goroutineCount)

	for i := 0; i < goroutineCount; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, err := db.Exec(`INSERT INTO test(value) VALUES(?)`,
					fmt.Sprintf("goroutine %d iteration %d", id, j))
				if err != nil {
					t.Errorf("Insert error: %v", err)
					return
				}

				/*
					SELECT query here causes SQLITE_LOCKED (6)
					https://www2.sqlite.org/cvstrac/wiki?p=DatabaseIsLocked

					https://github.com/mattn/go-sqlite3/issues/148#issuecomment-250905756

					var count int
					err = db.QueryRow(`SELECT COUNT(*) FROM test`).Scan(&count)
					if err != nil {
						t.Errorf("Query error: %v", err)
						return
					}
				*/

			}
		}(i)
	}

	wg.Wait()

	var total int
	err = db.QueryRow(`SELECT COUNT(*) FROM test`).Scan(&total)
	if err != nil {
		t.Errorf("Final count query error: %v", err)
	}
	expected := goroutineCount * iterations
	if total != expected {
		t.Errorf("expected %d rows, got %d", expected, total)
	}
}
