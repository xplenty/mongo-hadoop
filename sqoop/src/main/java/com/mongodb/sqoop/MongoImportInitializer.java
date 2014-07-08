package com.mongodb.sqoop;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.sqoop.configuration.ConnectionForm;
import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportForm;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.MutableContext;
import org.apache.sqoop.job.etl.Initializer;
import org.apache.sqoop.job.etl.InitializerContext;
import org.apache.sqoop.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MongoImportInitializer extends Initializer<MongoConnectionConfiguration, MongoImportJobConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoImportInitializer.class);

    private MongoExecutor executor;

    @Override
    public void initialize(final InitializerContext initializerContext, final MongoConnectionConfiguration connectionConfiguration,
                           final MongoImportJobConfiguration jobConfiguration) {
        ConnectionForm connectionForm = connectionConfiguration.getConnectionForm();
        String url = connectionForm.getUri();
        executor = new MongoExecutor(new MongoClientURI(url));
        try {
            configurePartitionProperties(initializerContext.getContext(), connectionConfiguration, jobConfiguration);
        } finally {
            executor.close();
        }
    }

    private void configurePartitionProperties(final MutableContext context, final MongoConnectionConfiguration connectionConfig,
                                              final MongoImportJobConfiguration jobConfig) {

        MongoImportForm collectionForm = jobConfig.getCollectionForm();
        String partitionField = collectionForm.getPartitionField();
        if (partitionField == null) {
            partitionField = "_id";
        }
        context.setString(MongoConnector.PARTITION_FIELD, partitionField);

        findMinMax(context, partitionField, collectionForm);
        //        splitVector(collectionForm);
    }

    private void splitVector(final MongoImportForm collectionForm) {
        DBCollection inputCollection = executor.getCollection(collectionForm.getDatabase(),
                                                              collectionForm.getCollection());
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("splitVector", inputCollection.getFullName())
                                                           .add("keyPattern", new BasicDBObject(collectionForm.getPartitionField(), 1))
                                                           .add("maxChunkSize", 16);
        final DBObject cmd = builder.get();

        CommandResult data;
        boolean ok = true;
        try {
            data = inputCollection.getDB().getSisterDB("admin").command(cmd);
        } catch (MongoException e) {  // 2.0 servers throw exceptions rather than info in a CommandResult
            data = null;
            LOG.info(e.getMessage(), e);
            if (e.getMessage().contains("unrecognized command: splitVector")) {
                ok = false;
            } else {
                throw e;
            }
        }

        if (data != null) {
            if (data.containsField("$err")) {
                //                throw new SqoopException("Error calculating splits: " + data);
            } else if (!data.get("ok").equals(1.0)) {
                ok = false;
            }
        }

        if (!ok) {
            CommandResult stats = inputCollection.getStats();
            if (stats.containsField("primary")) {
                DBCursor shards = inputCollection.getDB().getSisterDB("config")
                                                 .getCollection("shards")
                                                 .find(new BasicDBObject("_id", stats.getString("primary")));
                try {
                    if (shards.hasNext()) {
                        DBObject shard = shards.next();
                        MongoClientURI shardHost = new MongoClientURIBuilder(executor.getUri())
                                                       .host((String) shard.get("host"))
                                                       .build();
                        MongoClient shardClient = null;
                        try {
                            shardClient = new MongoClient(shardHost);
                            data = shardClient.getDB("admin").command(cmd);
                        } catch (UnknownHostException e) {
                            LOG.error(e.getMessage(), e);
                        } finally {
                            if (shardClient != null) {
                                shardClient.close();
                            }
                        }
                    }
                } finally {
                    shards.close();
                }
            }
            if (!data.get("ok").equals(1.0)) {
                //                throw new SqoopException("Unable to calculate input splits: " + data.get("errmsg"));
            }
        }

        // Comes in a format where "min" and "max" are implicit
        // and each entry is just a boundary key; not ranged
        DBObject splitKeys = (DBObject) data.get("splitKeys");

        LOG.debug("cmd = " + cmd);
        LOG.debug("data = " + data);
        LOG.debug("splitKeys = " + splitKeys);
    }

    private void findMinMax(final MutableContext context, final String partitionField, final MongoImportForm collectionForm) {
        List<DBObject> pipeline = new ArrayList<DBObject>();

        pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", "values")
                                                     .append("min", new BasicDBObject("$min", "$" + partitionField))
                                                     .append("max", new BasicDBObject("$max", "$" + partitionField))));
        Cursor aggregate = executor.getCollection(collectionForm.getDatabase(), collectionForm.getCollection())
                                   .aggregate(pipeline, AggregationOptions.builder().build());
        if (aggregate.hasNext()) {
            DBObject next = aggregate.next();
            String fieldType;
            Object min = next.get("min");
            if (min instanceof Integer) {
                context.setInteger(MongoConnector.PARTITION_MIN_VALUE, (Integer) min);
                context.setInteger(MongoConnector.PARTITION_MAX_VALUE, (Integer) next.get("max"));
                context.setString(MongoConnector.PARTITION_FIELD_TYPE, Integer.class.getName());
            } else if (min instanceof Long) {
                context.setLong(MongoConnector.PARTITION_MIN_VALUE, (Long) min);
                context.setLong(MongoConnector.PARTITION_MAX_VALUE, (Long) next.get("max"));
                context.setString(MongoConnector.PARTITION_FIELD_TYPE, Long.class.getName());
            } else if (min instanceof Double) {
                context.setString(MongoConnector.PARTITION_MIN_VALUE, Double.toString((Double) min));
                context.setString(MongoConnector.PARTITION_MAX_VALUE, Double.toString((Double) next.get("max")));
                context.setString(MongoConnector.PARTITION_FIELD_TYPE, Double.class.getName());
            } else if (min instanceof Date) {
                context.setString(MongoConnector.PARTITION_MIN_VALUE, DateFormat.getDateTimeInstance().format(min));
                context.setString(MongoConnector.PARTITION_MAX_VALUE, DateFormat.getDateTimeInstance().format(next.get("max")));
                context.setString(MongoConnector.PARTITION_FIELD_TYPE, Date.class.getName());
            } else {
                context.setString(MongoConnector.PARTITION_MIN_VALUE, min.toString());
                context.setString(MongoConnector.PARTITION_MAX_VALUE, next.get("max").toString());
                context.setString(MongoConnector.PARTITION_FIELD_TYPE, String.class.getName());
            }

        }
    }


    @Override
    public Schema getSchema(final InitializerContext context, final MongoConnectionConfiguration connectionConfiguration,
                            final MongoImportJobConfiguration importJobConfiguration) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
