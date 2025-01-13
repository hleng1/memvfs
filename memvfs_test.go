package memvfs_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/hleng1/memvfs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

func TestMemvfs(t *testing.T) {
	store := memvfs.NewMemStore()

	// TODO use New()
	memVFS := &memvfs.MemVFS{
		Name:  "memvfs",
		Store: store,
	}

	if err := sqlite3vfs.RegisterVFS("memvfs", memVFS); err != nil {
		t.Fatalf("Failed to register VFS: %v", err)
	}

	db, err := sql.Open("sqlite3", "file:test.db?vfs=memvfs&cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

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

	_, err = memVFS.Access("test.db", sqlite3vfs.AccessExists)
	if err != nil {
		t.Logf("Failed to access %v: %v\n", "test.db", err)
	}

	// TODO more tests like https://github.com/psanford/donutdb/blob/main/donutdb_test.go

	if err := memVFS.Delete("test.db", true); err != nil {
		t.Fatalf("Failed to delete test.db: %v", err)
	}
}
