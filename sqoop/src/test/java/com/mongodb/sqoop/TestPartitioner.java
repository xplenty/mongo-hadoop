package com.mongodb.sqoop;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static java.lang.String.format;

public class TestPartitioner {

    private MongoClientURI uri;
    private MongoClient mongoClient;
    private DBCollection collection;

    @Before
    public void createData() throws UnknownHostException {
        uri = BaseHadoopTest.authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", "sqoop"))
                            .build();
        mongoClient = new MongoClient(uri);

        collection = mongoClient.getDB(uri.getDatabase()).getCollection(uri.getCollection());
        collection.drop();
        collection.createIndex(new BasicDBObject("age", 1));
    }

    @Test
    public void testIntegerPartition() throws Exception {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        String key = "age";
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            int age = random.nextInt(80);
            min = Math.min(min, age);
            max = Math.max(max, age);
            collection.insert(new BasicDBObject(key, age));
        }

        runTest(min, max, key, false);
    }

    @Test
    public void testDatePartition() throws Exception {
        Date min = null;
        Date max = null;
        Calendar start = Calendar.getInstance();
        String key = "birthDate";
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            start.add(Calendar.HOUR, random.nextInt(100));
            min = min(min, start.getTime());
            max = max(max, start.getTime());
            collection.insert(new BasicDBObject(key, start.getTime()));
        }

        runTest(min, max, key, false);
    }

    @Test
    public void testLongPartition() throws Exception {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        String key = "age";
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            long age = random.nextInt(80);
            min = Math.min(min, age);
            max = Math.max(max, age);
            collection.insert(new BasicDBObject(key, age));
        }

        runTest(min, max, key, false);
    }

    @Test
    public void testDoublePartition() throws Exception {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        String key = "age";
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            double age = random.nextInt(80);
            min = Math.min(min, age);
            max = Math.max(max, age);
            collection.insert(new BasicDBObject(key, age));
        }

        runTest(min, max, key, false);
    }

    @Test
    public void testFloatPartition() throws Exception {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        String key = "age";
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            float age = random.nextInt(80);
            min = Math.min(min, age);
            max = Math.max(max, age);
            collection.insert(new BasicDBObject(key, age));
        }

        runTest(min, max, key, false);
    }

    @Test
    public void testStringPartition() throws Exception {
        String min = null;
        String max = null;
        String key = "name";
        Random random = new Random();
        String[] names = new String[]{"Kandice", "Mazie", "Annemarie", "Iraida", "Everett", "Wilford", "Hanna", "King", "Wynona", "Reyna",
                                      "Princess", "Kamilah", "Tempie", "Trent", "Lucretia", "Dana", "Loree", "Judy", "Christie", "Beula",
                                      "Lottie", "Meghann", "Eric", "Corazon", "Arlena", "Gustavo", "Rich", "Savannah", "Davis", "Will",
                                      "Reatha", "Sonia", "Ian", "Palma", "Marty", "Audrea", "Cedric", "Tad", "Kerry", "Dusti", "Kenya",
                                      "Gabriela", "Johna", "Ellie", "Anh", "Robbin", "Bradley", "Florida", "Gerri", "Jessie"};

        for (String name : names) {
            min = min(min, name);
            max = max(max, name);
            collection.insert(new BasicDBObject(key, name));
        }

        runTest(min, max, key, true);
    }

    private String min(final String left, final String right) {
        if (left == null) {
            return right;
        } else if (left.compareTo(right) < 0) {
            return left;
        } else {
            return right;
        }
    }

    private String max(final String left, final String right) {
        if (left == null) {
            return right;
        } else if (left.compareTo(right) < 0) {
            return right;
        } else {
            return left;
        }
    }

    private Date min(final Date left, final Date right) {
        if (left == null) {
            return right;
        } else if (left.before(right)) {
            return left;
        } else {
            return right;
        }
    }

    private Date max(final Date left, final Date right) {
        if (left == null) {
            return right;
        } else if (left.before(right)) {
            return right;
        } else {
            return left;
        }
    }

    public void runTest(final Object min, final Object max, final String partitionField, final boolean unsplittable) {
        MongoImportInitializer initializer = new MongoImportInitializer();

        MongoConnectionConfiguration connConf = new MongoConnectionConfiguration();
        MongoImportJobConfiguration jobConf = new MongoImportJobConfiguration();
        connConf.getConnectionForm().setUri(uri.toString());
        jobConf.getCollectionForm().setDatabase(uri.getDatabase());
        jobConf.getCollectionForm().setCollection(uri.getCollection());
        jobConf.getCollectionForm().setPartitionField(partitionField);

        InitializerContext initializerContext = new InitializerContext(new MutableMapContext());
        initializer.initialize(initializerContext, connConf, jobConf);

        MongoPartitioner partitioner = new MongoPartitioner();
        for (int maxPartitions = -5; maxPartitions < 15; maxPartitions++) {
            int normalizedMax = unsplittable || maxPartitions < 1 ? 1 : maxPartitions;
            PartitionerContext partitionerContext = new PartitionerContext(initializerContext.getContext(), maxPartitions, null);

            List<Partition> partitions = partitioner.getPartitions(partitionerContext, connConf, jobConf);
            Assert.assertEquals(normalizedMax, partitions.size());

            MongoPartition actual = (MongoPartition) partitions.get(0);
            String message = format("For partition count %d, Should find the min value (%s) in the condition [%s]", maxPartitions,
                                    min, actual);
            Assert.assertEquals(message, min.toString(), actual.getMin().toString());

            actual = (MongoPartition) partitions.get(normalizedMax - 1);
            Assert.assertEquals(format("For partition count %d, Should find the max value (%s) in the condition [%s]", maxPartitions,
                                       max, actual), max.toString(), actual.getMax().toString());
        }
    }

}
