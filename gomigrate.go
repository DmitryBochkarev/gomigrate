package gomigrate

import (
	"database/sql"
	"log"
)

type MigrateFunction func(*sql.Tx) error

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
			log.Printf("Last migration number: %d", lastMigration)
		} else {
			log.Println("Create database schema")
		}
		if up {
			log.Printf("Run migrations with step %d\n", step)
		} else {
			log.Printf("Run rollback migrations with step %d\n", step)
		}
		for i := migrationStartN; i != migrationEndN; i = i + migrationIncrement {
			log.Println("Begin")
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			log.Printf("Run migration %d %s", i, migrations[i].Name)
			if up {
				err = migrations[i].Up(tx)
			} else {
				err = migrations[i].Down(tx)
			}
			if err != nil {
				log.Printf("Fail migration %d %s", i, migrations[i].Name)
				log.Println("Rollback")
				tx.Rollback()
				return err
			}
			log.Printf("Success migration %d %s", i, migrations[i].Name)
			log.Println("Save migration number to schema_migrations")
			if up {
				_, err = tx.Exec(`insert into schema_migrations
					(id, title)
					values
					($1, $2)`, i, migrations[i].Name)
			} else {
				_, err = tx.Exec(`delete from schema_migrations where id = $1`, i)
			}
			if err != nil {
				log.Printf("Fail to save migration %d %s", i, migrations[i].Name)
				log.Println("Rollback")
				tx.Rollback()
				return err
			}
			log.Println("Commit")
			tx.Commit()
		}
		log.Println("All migrations success")
	}
	return nil
}

func migrationDatabaseSchemaCreate(db *sql.DB) error {
	log.Println("Check schema_migrations table")
	var count uint
	res := db.QueryRow(`select count(*) c from pg_stat_user_tables where relname = 'schema_migrations'`)
	err := res.Scan(&count)
	if err != nil {
		return err
	}
	if count == 1 {
		return nil
	}
	log.Println("Create schema_migrations table")
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
