package com.juanberger.hadoop;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvToMongo {

    // Adjust the BATCH_SIZE based on your specific needs and MongoDB's limitations
    private static final int BATCH_SIZE = 1000;

    public void uploadDataToMongoDB(String uri, String dbName, String collectionName, String filePath) {
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            List<Document> documents = new ArrayList<>();
            
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Document document = new Document();
                for (int i = 0; i < values.length; i++) {
                    document.append("field" + i, values[i]);
                }
                documents.add(document);

                // When the batch size is reached, insert all documents and clear the list
                if (documents.size() >= BATCH_SIZE) {
                    collection.insertMany(documents);
                    documents.clear();
                }
            }
            
            // Insert any remaining documents that didn't fill up the last batch
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Usage: CsvToMongo <uri> <dbName> <filePath> <collectionName>");
            return;
        }
        String uri = args[0];
        String dbName = args[1];
        String filePath = args[2];
        String collectionName = args[3];
        new CsvToMongo().uploadDataToMongoDB(uri, dbName, collectionName, filePath);
    }
}
