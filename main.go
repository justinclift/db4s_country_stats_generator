package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	sqlite "github.com/gwenn/gosqlite"
	"github.com/jackc/pgx"
	"github.com/mitchellh/go-homedir"
)

// Configuration file
type TomlConfig struct {
	Pg PGInfo
}
type PGInfo struct {
	Database       string
	NumConnections int `toml:"num_connections"`
	Port           int
	Password       string
	Server         string
	SSL            bool
	Username       string
}

var (
	// Application config
	Conf TomlConfig

	// Display debugging messages?
	debug = true

	// PostgreSQL Connection pool
	pg *pgx.ConnPool

	// SQLite pieces
	sdb *sqlite.Conn
)

func main() {
	// Override config file location via environment variables
	var err error
	configFile := os.Getenv("CONFIG_FILE")
	if configFile == "" {
		userHome, err := homedir.Dir()
		if err != nil {
			log.Fatalf("User home directory couldn't be determined: %s", "\n")
		}
		configFile = filepath.Join(userHome, ".db4s", "db4s_country_stats_generator.toml")
	}

	// Read our configuration settings
	if _, err = toml.DecodeFile(configFile, &Conf); err != nil {
		log.Fatal(err)
	}

	// Create the SQLite database
	dbFile := "db4s_country_stats.sqlite"
	sdb, err = sqlite.Open(dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = sdb.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	// Log successful connection
	if debug {
		fmt.Printf("Created country stats database: %v\n", dbFile)
	}

	// Setup the PostgreSQL config
	pgConfig := new(pgx.ConnConfig)
	pgConfig.Host = Conf.Pg.Server
	pgConfig.Port = uint16(Conf.Pg.Port)
	pgConfig.User = Conf.Pg.Username
	pgConfig.Password = Conf.Pg.Password
	pgConfig.Database = Conf.Pg.Database
	clientTLSConfig := tls.Config{InsecureSkipVerify: true}
	if Conf.Pg.SSL {
		pgConfig.TLSConfig = &clientTLSConfig
	} else {
		pgConfig.TLSConfig = nil
	}

	// Connect to PG
	pgPoolConfig := pgx.ConnPoolConfig{*pgConfig, Conf.Pg.NumConnections, nil, 5 * time.Second}
	pg, err = pgx.NewConnPool(pgPoolConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	// Log successful connection
	if debug {
		fmt.Printf("Connected to PostgreSQL server: %v\n", Conf.Pg.Server)
	}

	// Begin PostgreSQL transaction
	tx, err := pg.Begin()
	if err != nil {
		log.Fatal(err)
	}
	// Set up an automatic transaction roll back if the function exits without committing
	defer func() {
		err = tx.Rollback()
		if err != nil {
			log.Println(err)
		}
	}()

	// Determine start and end dates for the active users data
	var d, endDate, startDate time.Time
	dbQuery := `
		SELECT request_time
		FROM download_log
		WHERE request = '/currentrelease'
			AND client_country IS NOT NULL
			AND client_country != 'ZZZ'
			AND client_country != ''
		ORDER BY request_time ASC
		LIMIT 1`
	err = tx.QueryRow(dbQuery).Scan(&d)
	if err != nil {
		log.Fatalf("error when retrieving the start date for active users: %v\n", err)
	}
	startDate = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)

	if debug {
		fmt.Printf("Start date is: %s\n", startDate.Format(time.RFC822))
	}

	dbQuery = `
		SELECT request_time
		FROM download_log
		WHERE request = '/currentrelease'
			AND client_country IS NOT NULL
			AND client_country != 'ZZZ'
			AND client_country != ''
		ORDER BY request_time DESC
		LIMIT 1`
	err = tx.QueryRow(dbQuery).Scan(&d)
	if err != nil {
		log.Fatalf("error when retrieving the start date for active users: %v\n", err)
	}
	endDate = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)

	// Create SQLite tables to hold the active users
	sQuery := `
		DROP TABLE IF EXISTS "active_users";	
		CREATE TABLE IF NOT EXISTS "active_users" (
			"date"	TEXT,
			"country"	TEXT,
			"users"	INTEGER
		);
		CREATE INDEX IF NOT EXISTS "active_users-date_idx" ON "active_users" (
			"date"
		)`
	err = sdb.Exec(sQuery)
	if err != nil {
		log.Fatal(err)
	}

	// Create the SQLite prepared query for inserting the data rows
	insQuery := `
		INSERT INTO active_users (date, country, users)
		VALUES (?, ?, ?)`
	stmt, err := sdb.Prepare(insQuery)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Finalize()

	// For each 24 hour period, generate country count info & insert it in the SQLite database
	for queryDate := startDate; queryDate.Before(endDate); queryDate = queryDate.AddDate(0, 0, 1) {

		if debug {
			fmt.Printf("Generating active user data for: %s\n", queryDate.Format("2006-01-02"))
		}

		// Get the active user count for each country, for the queryDate 24 hour period
		dbQuery = `
			SELECT client_country, count(client_country)
			FROM download_log
			WHERE request_time > $1
				AND request_time < $2
				AND request = '/currentrelease'
				AND client_country IS NOT NULL
				AND client_country != 'ZZZ'
				AND client_country != ''
			GROUP BY client_country
			ORDER BY client_country ASC`
		rows, err := tx.Query(dbQuery, queryDate, queryDate.AddDate(0, 0, 1))
		if err != nil {
			log.Fatalf("Database query failed: %v\n", err)
		}
		list := make(map[string]int)
		for rows.Next() {
			var countryCode string
			var userCount int
			err = rows.Scan(&countryCode, &userCount)
			if err != nil {
				log.Fatalf("Error reading row data for country count.  Start date = '%s': %v\n", queryDate.Format(time.RFC822), err)
			}
			list[countryCode] = userCount
		}
		rows.Close()

		if debug {
			fmt.Printf("Inserting into SQLite database for: %s\n", queryDate.Format("2006-01-02"))
		}

		// Save the active users data to the SQLite database
		for cntry, users := range list {
			res, err := stmt.ExecDml(queryDate.Format("2006-01-02"), cntry, users)
			if err != nil {
				log.Fatal(err)
			}
			if res != 1 {
				log.Fatalf("Inserting into SQLite database returned '%d' instead of 1\n", res)
			}
		}
	}
}
