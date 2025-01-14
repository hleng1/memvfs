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
	dbName := "test.db"
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?vfs=memvfs&cache=shared", dbName))
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

	buf, err := v.GetFile(dbName)
	if err != nil {
		t.Error(err)
	}
	t.Log("Got buffer length:", len(buf))

	ok, _ := v.Access(dbName, sqlite3vfs.AccessExists)
	if !ok {
		t.Fatalf("Failed to access %v: %v\n", dbName, err)
	}

	// TODO more tests like https://github.com/psanford/donutdb/blob/main/donutdb_test.go

	db.Close()

	_, err = db.ExecContext(ctx, `INSERT INTO demo(data) VALUES ('Hello again from memvfs')`)
	if err == nil {
		t.Fatalf("%v should not allow insertion after Close: %v", dbName, err)
	}

	ok, _ = v.Access(dbName, sqlite3vfs.AccessExists)
	if ok {
		t.Fatalf("%v is still accessible after Close: %v", dbName, err)
	}
}

func TestConcurrentSingleDB(t *testing.T) {
	const (
		goroutineCount = 10
		iterations     = 100
	)

	db, err := sql.Open("sqlite3", "file:test_concurrent.db?&vfs=memvfs&cache=shared")
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
		t.Fatalf("Final count query error: %v", err)
	}
	expected := goroutineCount * iterations
	if total != expected {
		t.Fatalf("Expected %d rows, got %d", expected, total)
	}
}

func TestConcurrentMultiDB(t *testing.T) {
	const (
		goroutineCount = 20
		iterations     = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutineCount)

	for i := 0; i < goroutineCount; i++ {
		go func(id int) {
			defer wg.Done()
			dbName := fmt.Sprintf("test_concurrent_%2d.db", i)
			db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?&vfs=memvfs&cache=shared", dbName))
			if err != nil {
				t.Errorf("Failed to open DB: %v", err)
			}
			defer db.Close()

			_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS test (
            id INTEGER PRIMARY KEY,
            value TEXT
        )
    `)
			if err != nil {
				t.Errorf("Create table error: %v", err)
			}
			for j := 0; j < iterations; j++ {
				_, err := db.Exec(`INSERT INTO test(value) VALUES(?)`,
					fmt.Sprintf("goroutine %d iteration %d", id, j))
				if err != nil {
					t.Errorf("Insert error: %v", err)
					return
				}
			}
			var total int
			err = db.QueryRow(`SELECT COUNT(*) FROM test`).Scan(&total)
			if err != nil {
				t.Errorf("Final count query error: %v", err)
			}
			if total != iterations {
				t.Errorf("Expected %d rows, got %d", iterations, total)
			}
		}(i)
	}

	wg.Wait()
}
