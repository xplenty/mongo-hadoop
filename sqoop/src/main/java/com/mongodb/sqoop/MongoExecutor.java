package com.mongodb.sqoop;

import com.mongodb.AggregationOutput;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import org.apache.sqoop.common.SqoopException;

import java.net.UnknownHostException;
import java.util.Arrays;

public class MongoExecutor {

    private MongoClient mongoClient;
    private MongoClientURI uri;

    public MongoExecutor(final MongoClientURI uri) {
        this.uri = uri;
        try {
            mongoClient = new MongoClient(uri);
        } catch (UnknownHostException e) {
            throw new SqoopException(MongoConnectorError.MONGO_CONNECTOR_0000, e);
        }
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public DBObject aggregate(final String database, final String collectionName, final DBObject... projection) {
        DBCollection collection = mongoClient.getDB(database).getCollection(collectionName);
        AggregationOutput output = null;
        try {
            output = collection.aggregate(Arrays.asList(projection));
        } catch (MongoException e) {
            throw new SqoopException(MongoConnectorError.MONGO_CONNECTOR_0001, e);
        }
        return output.results().iterator().next();
    }

    public DBCollection getCollection(final String database, final String collection) {
        return mongoClient.getDB(database).getCollection(collection);
    }

    public MongoClientURI getUri() {
        return uri;
    }

    public void setUri(final MongoClientURI uri) {
        this.uri = uri;
    }
}
