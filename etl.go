package flow

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	mssql "github.com/microsoft/go-mssqldb"
)

// ETLOptions encapsulates batching and engine-specific performance tuning flags.
type ETLOptions struct {
	BatchSize        int       // Number of rows per batch flush
	Tablock          bool      // Acquire table lock for minimal logging on SQL Server
	CheckConstraints bool      // Enforce target constraints during MSSQL bulk insert
	FireTriggers     bool      // Execute target table triggers during MSSQL bulk insert
	KeepNulls        bool      // Preserve explicit NULL values during MSSQL bulk insert
	SrcConn          *sql.Conn // Optional pinned source connection (session scope)
	DstConn          *sql.Conn // Optional pinned destination connection (session scope)
	SrcTx            *sql.Tx   // Optional active source transaction
	DstTx            *sql.Tx   // Optional active destination transaction
}

// formatPlaceholder formats query placeholders based on the destination database driver syntax.
func formatPlaceholder(driver string, paramIdx int) string {
	switch strings.ToLower(driver) {
	case "postgres", "postgresql", "pgx", "pq":
		return "$" + strconv.Itoa(paramIdx)
	case "mysql", "sqlite", "sqlite3":
		return "?"
	case "oracle", "godror", "go-ora":
		return ":" + strconv.Itoa(paramIdx)
	case "sqlserver", "mssql":
		fallthrough
	default:
		return "@p" + strconv.Itoa(paramIdx)
	}
}

// maxParametersForDriver returns the maximum number of bind parameters supported per statement
// by the target database driver.
func maxParametersForDriver(driver string) int {
	switch strings.ToLower(driver) {
	case "postgres", "postgresql", "pgx", "pq", "mysql":
		return 65535
	case "sqlite", "sqlite3":
		return 32766
	case "oracle", "godror", "go-ora":
		return 32767
	case "sqlserver", "mssql":
		fallthrough
	default:
		return 2100
	}
}

// buildMultiRowInsertSQL constructs a parameterized multi-row INSERT statement using a
// pre-sized strings.Builder without intermediate slice allocations.
func buildMultiRowInsertSQL(driver, targetTable, colList string, numCols, numRows int) string {
	lowerDriver := strings.ToLower(driver)
	var b strings.Builder
	b.Grow(len(targetTable) + len(colList) + 25 + numRows*(numCols*5+3))
	b.WriteString("INSERT INTO ")
	b.WriteString(targetTable)
	b.WriteString(" (")
	b.WriteString(colList)
	b.WriteString(") VALUES ")

	paramIdx := 1
	for r := 0; r < numRows; r++ {
		if r > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < numCols; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			switch lowerDriver {
			case "postgres", "postgresql", "pgx", "pq":
				b.WriteByte('$')
				b.WriteString(strconv.Itoa(paramIdx))
			case "mysql", "sqlite", "sqlite3":
				b.WriteByte('?')
			case "oracle", "godror", "go-ora":
				b.WriteByte(':')
				b.WriteString(strconv.Itoa(paramIdx))
			default:
				b.WriteString("@p")
				b.WriteString(strconv.Itoa(paramIdx))
			}
			paramIdx++
		}
		b.WriteByte(')')
	}
	return b.String()
}

// flushMSSQLBatch performs a native TDS bulk stream copy into MSSQL with configured BulkOptions.
func flushMSSQLBatch(ctx context.Context, dstDB *sql.DB, targetTable string, cols []string, batchRows [][]interface{}, opts ETLOptions) error {
	if len(batchRows) == 0 {
		return nil
	}

	var tx *sql.Tx
	var err error
	ownsTx := false

	if opts.DstTx != nil {
		tx = opts.DstTx
	} else if opts.DstConn != nil {
		tx, err = opts.DstConn.BeginTx(ctx, nil)
		ownsTx = true
	} else {
		tx, err = dstDB.BeginTx(ctx, nil)
		ownsTx = true
	}
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	if ownsTx {
		defer tx.Rollback()
	}

	bulkOpts := mssql.BulkOptions{
		Tablock:          opts.Tablock,
		RowsPerBatch:     len(batchRows),
		CheckConstraints: opts.CheckConstraints,
		FireTriggers:     opts.FireTriggers,
		KeepNulls:        opts.KeepNulls,
	}

	bulkStmt := mssql.CopyIn(targetTable, bulkOpts, cols...)
	stmt, err := tx.PrepareContext(ctx, bulkStmt)
	if err != nil {
		return fmt.Errorf("failed to prepare bulk insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range batchRows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("bulk copy row execution error: %w", err)
		}
	}

	// Finalize bulk copy stream
	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("failed to finalize bulk insert: %w", err)
	}

	if ownsTx {
		return tx.Commit()
	}
	return nil
}

// StreamETL streams query results line-by-line from a source database into a target database table.
func StreamETL(ctx context.Context, r *Registry, srcDBName, queryStr, dstDBName, targetTable string, opts ETLOptions) (int64, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	queryStr = r.InterpolateVars(queryStr)

	srcHandle, err := r.GetDBHandle(srcDBName)
	if err != nil {
		return 0, fmt.Errorf("source db error: %w", err)
	}
	srcDB := srcHandle.Conn

	dstHandle, err := r.GetDBHandle(dstDBName)
	if err != nil {
		return 0, fmt.Errorf("destination db error: %w", err)
	}
	dstDB := dstHandle.Conn
	dstDriver := dstHandle.Driver

	var rows *sql.Rows
	if opts.SrcTx != nil {
		rows, err = opts.SrcTx.QueryContext(ctx, queryStr)
	} else if opts.SrcConn != nil {
		rows, err = opts.SrcConn.QueryContext(ctx, queryStr)
	} else {
		rows, err = srcDB.QueryContext(ctx, queryStr)
	}
	if err != nil {
		return 0, fmt.Errorf("source query error: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve columns: %w", err)
	}

	numCols := len(cols)
	if numCols == 0 {
		return 0, fmt.Errorf("query returned 0 columns")
	}

	colList := strings.Join(cols, ", ")
	lowerDstDriver := strings.ToLower(dstDriver)
	isMSSQL := lowerDstDriver == "sqlserver" || lowerDstDriver == "mssql"

	// Enforce dialect-aware parameter limit for non-MSSQL multi-row INSERT statements
	if !isMSSQL {
		maxParams := maxParametersForDriver(dstDriver)
		maxBatchRows := maxParams / numCols
		if maxBatchRows <= 0 {
			maxBatchRows = 1
		}
		if batchSize > maxBatchRows {
			batchSize = maxBatchRows
		}
	}

	var totalInserted int64 = 0
	batchRows := make([][]interface{}, 0, batchSize)
	rowArena := make([]interface{}, batchSize*numCols)
	scans := make([]interface{}, numCols)
	var valueArgs []interface{}
	var fullBatchSQL string
	var fullBatchStmt *sql.Stmt
	defer func() {
		if fullBatchStmt != nil {
			_ = fullBatchStmt.Close()
		}
	}()

	if !isMSSQL {
		valueArgs = make([]interface{}, 0, batchSize*numCols)
	}

	flushBatch := func() error {
		nRows := len(batchRows)
		if nRows == 0 {
			return nil
		}

		if isMSSQL {
			if err := flushMSSQLBatch(ctx, dstDB, targetTable, cols, batchRows, opts); err != nil {
				return fmt.Errorf("batch bulk copy error on table '%s': %w", targetTable, err)
			}
		} else {
			valueArgs = valueArgs[:0]
			for _, row := range batchRows {
				valueArgs = append(valueArgs, row...)
			}

			var execErr error
			if nRows == batchSize {
				if fullBatchSQL == "" {
					fullBatchSQL = buildMultiRowInsertSQL(dstDriver, targetTable, colList, numCols, batchSize)
				}
				if fullBatchStmt == nil {
					if opts.DstTx != nil {
						fullBatchStmt, execErr = opts.DstTx.PrepareContext(ctx, fullBatchSQL)
					} else if opts.DstConn != nil {
						fullBatchStmt, execErr = opts.DstConn.PrepareContext(ctx, fullBatchSQL)
					} else {
						fullBatchStmt, execErr = dstDB.PrepareContext(ctx, fullBatchSQL)
					}
				}
				if execErr == nil && fullBatchStmt != nil {
					_, execErr = fullBatchStmt.ExecContext(ctx, valueArgs...)
				} else {
					// Fallback to direct execution if driver does not support PrepareContext
					execErr = nil
					if opts.DstTx != nil {
						_, execErr = opts.DstTx.ExecContext(ctx, fullBatchSQL, valueArgs...)
					} else if opts.DstConn != nil {
						_, execErr = opts.DstConn.ExecContext(ctx, fullBatchSQL, valueArgs...)
					} else {
						_, execErr = dstDB.ExecContext(ctx, fullBatchSQL, valueArgs...)
					}
				}
			} else {
				insertSQL := buildMultiRowInsertSQL(dstDriver, targetTable, colList, numCols, nRows)
				if opts.DstTx != nil {
					_, execErr = opts.DstTx.ExecContext(ctx, insertSQL, valueArgs...)
				} else if opts.DstConn != nil {
					_, execErr = opts.DstConn.ExecContext(ctx, insertSQL, valueArgs...)
				} else {
					_, execErr = dstDB.ExecContext(ctx, insertSQL, valueArgs...)
				}
			}
			if execErr != nil {
				return fmt.Errorf("batch insert error on table '%s': %w", targetTable, execErr)
			}
		}

		totalInserted += int64(nRows)
		for i := 0; i < nRows*numCols; i++ {
			rowArena[i] = nil
		}
		batchRows = batchRows[:0]
		return nil
	}

	// When source and destination share the same single pinned connection or transaction,
	// rows must be fully read and closed before executing destination inserts on that same connection.
	sameSingleConn := srcDBName == dstDBName && (opts.SrcTx != nil || opts.DstTx != nil || opts.SrcConn != nil || opts.DstConn != nil)
	var bufferedRows [][]interface{}
	var bufferedVals []interface{}
	if sameSingleConn {
		bufferedVals = make([]interface{}, numCols)
		for i := range scans {
			scans[i] = &bufferedVals[i]
		}
	}

	for rows.Next() {
		select {
		case <-ctx.Done():
			return totalInserted, ctx.Err()
		default:
		}

		if sameSingleConn {
			if err := rows.Scan(scans...); err != nil {
				return totalInserted, fmt.Errorf("row scan error: %w", err)
			}
			rowCopy := make([]interface{}, numCols)
			for i, v := range bufferedVals {
				if b, ok := v.([]byte); ok {
					if utf8.Valid(b) {
						rowCopy[i] = string(b)
					} else {
						cloned := make([]byte, len(b))
						copy(cloned, b)
						rowCopy[i] = cloned
					}
				} else {
					rowCopy[i] = v
				}
				bufferedVals[i] = nil
			}
			bufferedRows = append(bufferedRows, rowCopy)
		} else {
			rowIdx := len(batchRows)
			rowSlice := rowArena[rowIdx*numCols : (rowIdx+1)*numCols]
			for i := 0; i < numCols; i++ {
				scans[i] = &rowSlice[i]
			}
			if err := rows.Scan(scans...); err != nil {
				return totalInserted, fmt.Errorf("row scan error: %w", err)
			}
			for i, v := range rowSlice {
				if b, ok := v.([]byte); ok {
					if utf8.Valid(b) {
						rowSlice[i] = string(b)
					} else {
						cloned := make([]byte, len(b))
						copy(cloned, b)
						rowSlice[i] = cloned
					}
				}
			}
			batchRows = append(batchRows, rowSlice)
			if len(batchRows) >= batchSize {
				if err := flushBatch(); err != nil {
					return totalInserted, err
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return totalInserted, fmt.Errorf("rows iteration error: %w", err)
	}
	_ = rows.Close()

	if sameSingleConn {
		for _, r := range bufferedRows {
			batchRows = append(batchRows, r)
			if len(batchRows) >= batchSize {
				if err := flushBatch(); err != nil {
					return totalInserted, err
				}
			}
		}
	}
	if err := flushBatch(); err != nil {
		return totalInserted, err
	}

	return totalInserted, nil
}
