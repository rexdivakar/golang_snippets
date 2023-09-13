package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	// Load environment variables from the .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Read the PostgreSQL hostname and password from environment variables
	hostname := os.Getenv("PG_HOSTNAME")
	dbname := os.Getenv("PG_DBNAME")
	password := os.Getenv("PG_PASSWORD")
	username := os.Getenv("PG_USERNAME")
	port := os.Getenv("PG_PORT")
	sslmode := os.Getenv("SSL_MODE")

	// Define a command-line flag for the output CSV file path
	var outputFilePath string
	flag.StringVar(&outputFilePath, "output", "output.csv", "Output CSV file path")

	// Define a command-line flag for the input SQL query file path
	var queryFilePath string
	flag.StringVar(&queryFilePath, "query", "input.dat", "Input SQL query file path")

	flag.Parse()

	// Read the SQL query from the input file
	query, err := readQueryFromFile(queryFilePath)
	if err != nil {
		log.Fatal(err)
	}

	// Construct the PostgreSQL connection string
	connectionString := fmt.Sprintf("user=%s dbname=%s host=%s password=%s port=%s sslmode=%s", username, dbname, hostname, password, port, sslmode)

	// Open a connection to the PostgreSQL database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Perform the database query to fetch the data
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Create a CSV file to write the data
	csvFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	// Create a CSV writer
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// Get column names and write them as the header row
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	if err := csvWriter.Write(columns); err != nil {
		log.Fatal(err)
	}

	// Iterate through the rows and write them to the CSV file
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal(err)
		}
		if err := csvWriter.Write(valuesToStrings(values)); err != nil {
			log.Fatal(err)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Data exported to %s\n", outputFilePath)
}

// Helper function to convert interface{} values to strings
func valuesToStrings(values []interface{}) []string {
	strValues := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			strValues[i] = ""
		} else {
			strValues[i] = fmt.Sprintf("%v", v)
		}
	}
	return strValues
}

// Helper function to read the SQL query from a file
func readQueryFromFile(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var query string

	for scanner.Scan() {
		query += scanner.Text() + " "
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return query, nil
}
