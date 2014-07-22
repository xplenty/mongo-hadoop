package com.mongodb.sqoop;

import com.mongodb.DBObject;
import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.InitializerContext;
import org.junit.Assert;
import org.junit.Test;

public class TestMongoExtractor extends BaseSqoopTest {

    @Test
    public void testExtract() throws Exception {
        MongoImportInitializer initializer = new MongoImportInitializer();

        MongoConnectionConfiguration connConf = new MongoConnectionConfiguration();
        MongoImportJobConfiguration jobConf = new MongoImportJobConfiguration();
        connConf.getConnectionForm().setUri(getUri().toString());
        jobConf.getImportForm().setDatabase(getUri().getDatabase());
        jobConf.getImportForm().setCollection(getUri().getCollection());
        jobConf.getImportForm().setPartitionField("age");

        InitializerContext initializerContext = new InitializerContext(new MutableMapContext());

        initializer.initialize(initializerContext, connConf, jobConf);

        Extractor extractor = new MongoExtractor();
        testRange(connConf, jobConf, initializerContext, extractor, 40, 50);
        testRange(connConf, jobConf, initializerContext, extractor, -1, 2);
        testRange(connConf, jobConf, initializerContext, extractor, 600, 1000);
    }

    public void testRange(final MongoConnectionConfiguration connConf, final MongoImportJobConfiguration jobConf,
                          final InitializerContext initializerContext, final Extractor extractor, final int minValue, final int maxValue) {
        
        extractor.extract(new ExtractorContext(initializerContext.getContext(), new MockWriter(minValue, maxValue), null),
                          connConf, jobConf, new MongoPartition("age", minValue, maxValue));
    }

    private class MockWriter extends DataWriter {

        private final int minValue;
        private final int maxValue;

        public MockWriter(final int minValue, final int maxValue) {
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        public void writeArrayRecord(final Object[] objects) {
            Assert.assertTrue("Should only be a 1 element array", objects.length == 1);
            Assert.assertTrue("Should only have a DBObject", objects[0] instanceof DBObject);
            DBObject object = (DBObject) objects[0];
            Assert.assertTrue("Should be gte than 40", ((Integer) object.get("age")) >= minValue);
            Assert.assertTrue("Should be lt 50", ((Integer) object.get("age")) < maxValue);
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