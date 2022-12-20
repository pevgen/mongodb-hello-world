package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MongoCRUDProcessorTest {

    private static MongoCRUDProcessor mongoCRUDProcessor;
    private static MongoClient mongoClient;

    @BeforeAll
    static void init() {
        mongoClient = MongoClientBuilder.buildMongoClient();
        mongoCRUDProcessor = new MongoCRUDProcessor(mongoClient);
    }

    @AfterAll
    static void destroy() {
        mongoClient.close();
    }

    @Test
    void should_insert_one() {
        Assertions.assertNotNull(mongoCRUDProcessor.insertOne());
    }

    @Test
    void should_insert_many() {
        Assertions.assertEquals(2, mongoCRUDProcessor.insertMany());
    }
}