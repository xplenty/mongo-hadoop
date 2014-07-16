package com.mongodb.sqoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.InitializerContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMongoExtractor {

    private MongoClientURI uri;
    private MongoClient mongoClient;
    private DBCollection collection;

    @Before
    public void setUp() throws Exception {
        uri = BaseHadoopTest.authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", "sqoop"))
                            .build();
        mongoClient = new MongoClient(uri);

        collection = mongoClient.getDB(uri.getDatabase()).getCollection(uri.getCollection());
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

    @Test
    public void testExtract() throws Exception {
        MongoImportInitializer initializer = new MongoImportInitializer();

        MongoConnectionConfiguration connConf = new MongoConnectionConfiguration();
        MongoImportJobConfiguration jobConf = new MongoImportJobConfiguration();
        connConf.getConnectionForm().setUri(uri.toString());
        jobConf.getCollectionForm().setDatabase(uri.getDatabase());
        jobConf.getCollectionForm().setCollection(uri.getCollection());
        jobConf.getCollectionForm().setPartitionField("age");

        InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

        initializer.initialize(initializerContext, connConf, jobConf);
        MongoPartition partition;

        Extractor extractor = new MongoExtractor();
        ExtractorContext extractorContext = new ExtractorContext(initializerContext.getContext(), new MockWriter(), null);

        partition = new MongoPartition("age", 40, 50);
        extractor.extract(extractorContext, connConf, jobConf, partition);
    }

    private class MockWriter extends DataWriter {
        @Override
        public void writeArrayRecord(final Object[] objects) {
            Assert.assertTrue("Should only be a 1 element array", objects.length == 1);
            Assert.assertTrue("Should only have a DBObject", objects[0] instanceof DBObject);
        }

        @Override
        public void writeCsvRecord(final String s) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public void writeContent(final Object o, final int i) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public void setFieldDelimiter(final char c) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }
    }
}