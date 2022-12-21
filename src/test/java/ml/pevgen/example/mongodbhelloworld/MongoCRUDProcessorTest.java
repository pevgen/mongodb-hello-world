package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
        long countBefore = mongoCRUDProcessor.getCollectionDocumentCount();
        assertNotNull(mongoCRUDProcessor.insertOne());
        long countAfter = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(countBefore + 1, countAfter);
    }

    @Test
    void should_insert_many() {
        long countBefore = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(2, mongoCRUDProcessor.insertMany());
        long countAfter = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(countBefore + 2, countAfter);
    }

    @Test
    void should_delete_one() {

        long countBefore = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(1, mongoCRUDProcessor.deleteOne());
        long countAfter = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(countBefore - 1, countAfter);
    }

    @Test
    void should_delete_many() {
        mongoCRUDProcessor.deleteAll();
        mongoCRUDProcessor.insertMany();
        long countBefore = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(2, mongoCRUDProcessor.deleteMany());
        long countAfter = mongoCRUDProcessor.getCollectionDocumentCount();
        assertEquals(countBefore - 2, countAfter);
    }

    @Test
    void should_find_one() {
        mongoCRUDProcessor.deleteAll();
        mongoCRUDProcessor.insertMany();
        assertNotNull(mongoCRUDProcessor.findOne());
    }


    @Test
    void should_find_many() {
        mongoCRUDProcessor.deleteAll();
        mongoCRUDProcessor.insertMany();
        assertEquals(2, mongoCRUDProcessor.findMany());
    }
}