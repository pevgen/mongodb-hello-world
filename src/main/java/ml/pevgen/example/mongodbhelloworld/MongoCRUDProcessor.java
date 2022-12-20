package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class MongoCRUDProcessor {

    private static final String COLLECTION_NAME_ACCOUNTS = "accounts";
    private static final String DATABASE_NAME = "test";


    private final MongoClient mongoClient;

    public MongoCRUDProcessor(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }


    public BsonValue insertOne() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Document inspection = new Document("_id", new ObjectId())
                .append("id", "10021-2015-ENFO")
                .append("certificate_number", 9278806)
                .append("business_name", "ATLIXCO DELI GROCERY INC.")
                .append("date", Date.from(LocalDate.of(2015, 2, 20).atStartOfDay(ZoneId.systemDefault()).toInstant()))
                .append("result", "No Violation Issued")
                .append("sector", "Cigarette Retail Dealer - 127")
                .append("address", new Document().append("city", "RIDGEWOOD").append("zip", 11385).append("street", "MENAHAN ST").append("number", 1712));
        InsertOneResult result = collection.insertOne(inspection);
        BsonValue id = result.getInsertedId();
        System.out.println(id);
        return id;
    }

    public int insertMany() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Document doc1 = new Document().append("account_holder", "john doe").append("account_id", "MDB99115881").append("balance", 1785).append("account_type", "checking");
        Document doc2 = new Document().append("account_holder", "jane doe").append("account_id", "MDB79101843").append("balance", 1468).append("account_type", "checking");
        List<Document> accounts = List.of(doc1, doc2);
        InsertManyResult result = collection.insertMany(accounts);
        result.getInsertedIds()
                .forEach((x, y) -> System.out.println(y.asObjectId()));
        return result.getInsertedIds().size();
    }

    public void updateOne() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Bson query = Filters.eq("account_id", "MDB12234728");
        Bson updates = Updates.combine(Updates.set("account_status", "active"), Updates.inc("balance", 100));
        UpdateResult upResult = collection.updateOne(query, updates);
        System.out.println(upResult.getModifiedCount());
    }

    public void updateMany() {

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Bson query = Filters.eq("account_type", "savings");
        Bson updates = Updates.combine(Updates.set("minimum_balance", 100));
        UpdateResult upResult = collection.updateMany(query, updates);
        System.out.println(upResult.getModifiedCount());
    }


    public void findMany() {

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        try (MongoCursor<Document> cursor = collection.find(
                        and(
                                gte("balance", 1000),
                                eq("account_type", "checking"))
                )
                .iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
    }

    public void findOne() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Document doc = collection.find(
                        and(
                                gte("balance", 1000),
                                eq("account_type", "checking")))
                .first();
        System.out.println(doc.toJson());
    }

    public void deleteOne() {

        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Bson query = Filters.eq("account_holder", "john doe");
        DeleteResult delResult = collection.deleteOne(query);
        System.out.println("Deleted a document:");
        System.out.println("\t" + delResult.getDeletedCount());

    }

    public void deleteMany() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME_ACCOUNTS);
        Bson query = eq("account_status", "dormant");
        DeleteResult delResult = collection.deleteMany(query);
        System.out.println(delResult.getDeletedCount());
    }

    /**
     * default - 60 sec transaction timeout
     */
    public void manyDocUpdateTransaction() {

        final ClientSession clientSession = mongoClient.startSession();

        TransactionBody<String> txnBody = () -> {
            MongoCollection<Document> bankingCollection =
                    mongoClient.getDatabase(DATABASE_NAME)
                            .getCollection(COLLECTION_NAME_ACCOUNTS);

            Bson fromAccount = eq("account_id", "MDB310054629");
            Bson withdrawal = Updates.inc("balance", -200);

            Bson toAccount = eq("account_id", "MDB643731035");
            Bson deposit = Updates.inc("balance", 200);

            System.out.println("This is from Account " + fromAccount.toBsonDocument().toJson() + " withdrawn " + withdrawal.toBsonDocument().toJson());
            System.out.println("This is to Account " + toAccount.toBsonDocument().toJson() + " deposited " + deposit.toBsonDocument().toJson());
            bankingCollection.updateOne(clientSession, fromAccount, withdrawal);
            bankingCollection.updateOne(clientSession, toAccount, deposit);

            return "Transferred funds from John Doe to Mary Doe";
        };

        try {
            clientSession.withTransaction(txnBody);
        } catch (RuntimeException e) {
            System.out.println(e);
        } finally {
            clientSession.close();
        }

    }

}
