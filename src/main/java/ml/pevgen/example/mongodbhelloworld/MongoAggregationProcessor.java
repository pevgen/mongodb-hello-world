package ml.pevgen.example.mongodbhelloworld;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.List.of;

public class MongoAggregationProcessor {


    private static final String COLLECTION_NAME_ACCOUNTS = "accounts";
    private static final String DATABASE_NAME = "test";


    private final MongoClient mongoClient;

    public MongoAggregationProcessor(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public void matchStage(){
        MongoCollection<Document> accounts = getMongoCollection();
        Bson matchStage = Aggregates.match(
                Filters.eq("account_id", "MDB310054629"));
        System.out.println("Display aggregation results");
        accounts.aggregate(of(matchStage))
                .forEach(document->System.out.print(document.toJson()));
    }

    public void matchAndGroupStages(){
        MongoCollection<Document> accounts = getMongoCollection();
        Bson matchStage = Aggregates.match(
                Filters.eq("account_id", "MDB310054629"));
        Bson groupStage = Aggregates.group("$account_type",
                sum("total_balance", "$balance"),
                avg("average_balance", "$balance"));
        System.out.println("Display aggregation results");
        accounts.aggregate(of(matchStage, groupStage))
                .forEach(document->System.out.print(document.toJson()));
    }


    public void matchSortAndProjectStages(MongoCollection<Document> accounts){
        Bson matchStage = Aggregates.match(
                Filters.and(
                        Filters.gt("balance", 1500),
                        Filters.eq("account_type", "checking")));
        Bson sortStage = Aggregates.sort(
                Sorts.orderBy(
                        descending("balance")));
        Bson projectStage = Aggregates.project(
                Projections.fields(
                        Projections.include("account_id", "account_type", "balance"),
                        Projections.computed("euro_balance",
                                new Document("$divide", of("$balance", 1.20F))),
                        Projections.excludeId()));
        System.out.println("Display aggregation results");
        accounts.aggregate(of(matchStage,sortStage, projectStage))
                .forEach(document -> System.out.print(document.toJson()));
    }

    //
//     another example
//
//    public void showGBPBalancesForCheckingAccounts(MongoCollection<Document> accounts) {
//        Bson matchStage = Aggregates.match(
//                Filters.and(
//                        Filters.eq("account_type", "checking"),
//                        Filters.gt("balance", 1500)));
//        Bson sortStage = Aggregates.sort(
//                Sorts.orderBy(
//                        descending("balance")));
//        Bson projectStage = Aggregates.project(
//                Projections.fields(
//                        Projections.include("account_id", "account_type", "balance"),
//                        Projections.excludeId()));
//        System.out.println("Display aggregation results");
//        accounts.aggregate(of(matchStage,sortStage, projectStage))
//                .forEach(document -> System.out.print(document.toJson()));
//    }
    private MongoCollection<Document> getMongoCollection() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        return database.getCollection(COLLECTION_NAME_ACCOUNTS);
    }
}
