package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class Main {

    public static void main(String[] args) {


        try (MongoClient mongoClient = MongoClientBuilder.buildMongoClient()) {
            MongoDatabase database = mongoClient.getDatabase("test");
            System.out.println("Hello world! database: " + database);
            MongoCollection<Document> col = database.getCollection("test");
            System.out.println("col = " + col.find().first());
        }
    }
}