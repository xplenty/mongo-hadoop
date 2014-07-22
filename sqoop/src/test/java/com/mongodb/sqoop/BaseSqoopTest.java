package com.mongodb.sqoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.junit.After;
import org.junit.Before;

public class BaseSqoopTest extends BaseHadoopTest {
    private MongoClientURI uri;
    private MongoClient mongoClient;
    private DBCollection collection;

    @Before
    public void setUp() throws Exception {
        setUri(BaseHadoopTest.authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", "sqoop"))
                            .build());
        mongoClient = new MongoClient(getUri());

        collection = mongoClient.getDB(getUri().getDatabase()).getCollection(getUri().getCollection());
        collection.drop();
        collection.createIndex(new BasicDBObject("age", 1));

        for (int i = 0; i < 1000; i++) {
            collection.insert(new BasicDBObject("age", i));
        }
    }

    @After
    public void tearDown() throws Exception {
        collection.drop();
        mongoClient.close();
    }

    public MongoClientURI getUri() {
        return uri;
    }

    public void setUri(final MongoClientURI uri) {
        this.uri = uri;
    }
}
