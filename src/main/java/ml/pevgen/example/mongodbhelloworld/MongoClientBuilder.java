package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoClientBuilder {

    private static final String MONGODB_URI_PARAMETER = "mongodb.uri";

    public static MongoClient buildMongoClient() {
        String mongoUri = System.getProperty(MONGODB_URI_PARAMETER);

        ConnectionString connectionString = new ConnectionString(mongoUri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .serverApi(ServerApi.builder()
                        .version(ServerApiVersion.V1)
                        .build())
                .build();
        return MongoClients.create(settings);
    }

}
