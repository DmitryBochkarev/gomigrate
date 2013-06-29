package gomigrate

import (
	"database/sql"
)

type MigrateFunction func(tx *sql.Tx) error

type Migration struct {
	Name string
	Up   MigrateFunction
	Down MigrateFunction
}

func MigrateDatabase(db *sql.DB, migrations []Migration, up bool, step int) error {
	err := migrationDatabaseSchemaCreate(db)
	if err != nil {
		return err
	}
	var lastMigration int
	res := db.QueryRow(`select coalesce(max(id), -1) id from schema_migrations`)
	err = res.Scan(&lastMigration)
	if err != nil {
		return err
	}
	var runMigrations bool
	var migrationStartN int
	var migrationEndN int
	var migrationIncrement int
	if up {
		migrationStartN = lastMigration + 1
		migrationIncrement = 1
		if step > 0 {
			migrationEndN = migrationStartN + step
		} else {
			migrationEndN = len(migrations)
		}
		if migrationEndN > len(migrations) {
			migrationEndN = len(migrations)
		}
		runMigrations = migrationStartN < len(migrations)
	} else {
		migrationStartN = lastMigration
		migrationIncrement = -1
		if step > 0 {
			migrationEndN = lastMigration - step
		} else {
			migrationEndN = lastMigration - 1
		}
		if migrationEndN <= -1 {
			migrationEndN = -1
		}
		runMigrations = migrationStartN > -1
	}
	if runMigrations {
		if lastMigration > -1 {
//			log(LOG_INFO, "Last migration number: ", lastMigration)
		} else {
//			log(LOG_INFO, "Create database schema")
		}
//		log(LOG_INFO, "Run migrations in ", app.migrationDirection, " direction with step", app.migrationStep)
		for i := migrationStartN; i != migrationEndN; i = i + migrationIncrement {
//			log(LOG_INFO, "Begin")
			tx, err := db.Begin()
			if err != nil {
				return err
			}
//			log(LOG_INFO, "Run migration ", i, migrations[i].Name)
			if up {
				err = migrations[i].Up(tx)
			} else {
				err = migrations[i].Down(tx)
			}
			if err != nil {
//				log(LOG_ERROR, "Fail migration ", i)
//				log(LOG_ERROR, "Rollback")
				tx.Rollback()
				return err
			}
//			log(LOG_INFO, "Success migration ", i, migrations[i].Name)
//			log(LOG_INFO, "Save migration number to schema_migrations")
			if up {
				_, err = db.Exec(`insert into schema_migrations
					(id, title)
					values
					($1, $2)`, i, migrations[i].Name)
			} else {
				_, err = db.Exec(`delete from schema_migrations where id = $1`, i)
			}
			if err != nil {
//				log(LOG_ERROR, "Fail saving migration ", i, migrations[i].Name)
//				log(LOG_ERROR, "Rollback")
				tx.Rollback()
				return err
			}
//			log(LOG_INFO, "Commit")
			tx.Commit()
		}
//		log(LOG_INFO, "All migrations success")
	}
	return nil
}

func migrationDatabaseSchemaCreate(db *sql.DB) error {
//	log(LOG_INFO, "Check schema_migrations table")
	var count uint
	res := db.QueryRow(`select count(*) c from pg_stat_user_tables where relname = 'schema_migrations'`)
	err := res.Scan(&count)
	if err != nil {
		return err
	}
	if count == 1 {
		return nil
	}
//	log(LOG_INFO, "Create schema_migrations table")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		create table schema_migrations (
			id integer,
			title varchar,
			created_at timestamp not null default now()
		)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}
