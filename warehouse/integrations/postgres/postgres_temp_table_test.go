package postgres_test

import (
	"context"
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTempTableAccess(t *testing.T) {
	ctx := context.Background()

	// create a new database connection
	db, err := sql.Open("postgres", "postgres://rudder:password@localhost:6432/jobsdb?sslmode=disable")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// create temp table and insert rows
	t.Run("create temp table and insert rows", func(t *testing.T) {
		tempTableTxn, err := db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		defer func() { _ = tempTableTxn.Rollback() }()

		_, err = tempTableTxn.ExecContext(ctx, `CREATE TEMP TABLE mytable (id serial, name varchar(20)) ON COMMIT PRESERVE ROWS`)
		require.NoError(t, err)

		_, err = tempTableTxn.ExecContext(ctx, `INSERT INTO mytable (name) VALUES ($1), ($2)`, "Alice", "Bob")
		require.NoError(t, err)

		err = tempTableTxn.Commit()
		require.NoError(t, err)
	})

	// access temp table and get row count
	t.Run("access temp table and get row count", func(t *testing.T) {
		accessTempTableTxn, err := db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		defer func() { _ = accessTempTableTxn.Rollback() }()

		_, err = accessTempTableTxn.ExecContext(ctx, `CREATE TEMP TABLE mytable1 AS (SELECT * FROM mytable)`)
		require.NoError(t, err)

		var count int
		err = accessTempTableTxn.QueryRowContext(ctx, `SELECT COUNT(*) FROM mytable1`).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 2, count)

		err = accessTempTableTxn.Commit()
		require.NoError(t, err)
	})
}

//func TestTempTableAccessMulti(t *testing.T) {
//	var (
//		dbs []*sql.DB
//
//		connections = 100
//		ctx         = context.Background()
//	)
//	for i := 0; i < connections; i++ {
//		// create a new database connection
//		db, err := sql.Open("postgres", "postgres://rudder:password@localhost:6432/jobsdb?sslmode=disable")
//		require.NoError(t, err)
//		defer func() { _ = db.Close() }()
//
//		dbs = append(dbs, db)
//	}
//
//	for _, db := range dbs {
//		// begin a new transaction and create a temporary table
//		tx1, err := db.BeginTx(ctx, &sql.TxOptions{})
//		require.NoError(t, err)
//		defer func() { _ = tx1.Rollback() }()
//
//		_, err = tx1.ExecContext(ctx, `CREATE TEMP TABLE mytable (id serial, name varchar(20)) ON COMMIT PRESERVE ROWS`)
//		require.NoError(t, err)
//
//		_, err = tx1.ExecContext(ctx, `INSERT INTO mytable (name) VALUES ($1), ($2)`, "Alice", "Bob")
//		require.NoError(t, err)
//
//		// commit the first transaction
//		err = tx1.Commit()
//		require.NoError(t, err)
//	}
//
//	for _, db := range dbs {
//		// begin a new transaction and access the temporary table
//		tx2, err := db.BeginTx(ctx, &sql.TxOptions{})
//		require.NoError(t, err)
//		defer func() { _ = tx2.Rollback() }()
//
//		var count int
//		err = tx2.QueryRowContext(ctx, `SELECT COUNT(*) FROM mytable`).Scan(&count)
//		require.NoError(t, err)
//		require.Equal(t, 2, count)
//
//		// commit the second transaction
//		err = tx2.Commit()
//		require.NoError(t, err)
//	}
//}
