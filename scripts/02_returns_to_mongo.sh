#!/bin/bash

# Path to the CSV files
STOCK_RETURNS_CSV="stock_returns.csv"

# MongoDB connection URI and database name
MONGO_URI=$(cat mongo_uri.txt)
DB_NAME="port_optim" # replace with your actual database name

# Collection names
STOCK_RETURNS_COLLECTION="stockReturns"

# Path to your compiled Java program with dependencies
JAVA_PROGRAM_PATH="/home/hadoop/Hadoop_Portfolio_Optimization/CsvToMongo/target/CsvToMongo-1.0-SNAPSHOT-jar-with-dependencies.jar"

# Upload stock_returns.csv
java -cp "$JAVA_PROGRAM_PATH" com.juanberger.hadoop.CsvToMongo "$MONGO_URI" "$DB_NAME" "$STOCK_RETURNS_CSV" "$STOCK_RETURNS_COLLECTION"
