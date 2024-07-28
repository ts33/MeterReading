package repo

import (
	sql "database/sql"
	// "fmt"

	model "github.com/ts33/energy-reading/.gen/postgres/public/model"
	table "github.com/ts33/energy-reading/.gen/postgres/public/table"
)

// BulkInsertMeterReadings takes in a list of MeterReadings and inserts them to the database as a single bulk insert.
// It assumes that the insert should happen if and only if there are no conflicts.
func BulkInsertMeterReadings(db *sql.DB, readings []*model.MeterReadings) error {

	insertStmt := table.MeterReadings.
		INSERT(table.MeterReadings.Nmi, table.MeterReadings.Timestamp, table.MeterReadings.Consumption).
		MODELS(readings).
		ON_CONFLICT(table.MeterReadings.ID).DO_NOTHING()

	// debugSQL := insertStmt.DebugSql()
	// fmt.Println(debugSQL)
	_, err := insertStmt.Exec(db)
	return err
}
