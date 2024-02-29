#!/bin/bash

# Path to the CSV files
PORTFOLIOS_CSV="portfolios.csv"
FINAL_RESULT_CSV="final_result.csv"

# MongoDB connection URI and database name
MONGO_URI=$(cat mongo_uri.txt)
DB_NAME="port_optim" # replace with your actual database name

# Collection names
PORTFOLIOS_COLLECTION="portfolios"
FINAL_RESULT_COLLECTION="finalResult"

# Path to your compiled Java program with dependencies
JAVA_PROGRAM_PATH="/home/hadoop/Hadoop_Portfolio_Optimization/CsvToMongo/target/CsvToMongo-1.0-SNAPSHOT-jar-with-dependencies.jar"

# Upload portfolios.csv
java -cp "$JAVA_PROGRAM_PATH" com.juanberger.hadoop.CsvToMongo "$MONGO_URI" "$DB_NAME" "$PORTFOLIOS_CSV" "$PORTFOLIOS_COLLECTION"

# Upload final_result.csv
java -cp "$JAVA_PROGRAM_PATH" com.juanberger.hadoop.CsvToMongo "$MONGO_URI" "$DB_NAME" "$FINAL_RESULT_CSV" "$FINAL_RESULT_COLLECTION"
