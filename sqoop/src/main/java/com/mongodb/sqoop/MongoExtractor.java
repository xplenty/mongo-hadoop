package com.mongodb.sqoop;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.sqoop.configuration.ConnectionForm;
import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportForm;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Partition;

import java.net.UnknownHostException;

public class MongoExtractor extends Extractor<MongoConnectionConfiguration, MongoImportJobConfiguration, Partition> {
    private long rowsRead = 0;

    @Override
    public void extract(final ExtractorContext context, final MongoConnectionConfiguration connectionConfiguration,
                        final MongoImportJobConfiguration jobConfiguration, final Partition partition) {
        ConnectionForm connectionForm = connectionConfiguration.getConnectionForm();
        String url = connectionForm.getUri();
        MongoClient client = null;
        DBCursor cursor = null;
        try {
            client = new MongoClient(new MongoClientURI(url));
            MongoImportForm collectionForm = jobConfiguration.getCollectionForm();
            DBCollection collection = client.getDB(collectionForm.getDatabase())
                                            .getCollection(collectionForm.getCollection());
            DBObject condition = ((MongoPartition) partition).getCondition();
            cursor = collection.find(condition);
            try {
                while (cursor.hasNext()) {
                    context.getDataWriter().writeArrayRecord(new Object[]{cursor.next()});
                    rowsRead++;
                }
            } finally {
                cursor.close();
            }
        } catch (UnknownHostException e) {
            throw new SqoopException(MongoConnectorError.MONGO_CONNECTOR_0000, e);
        } finally {
            if (client != null) {
                client.close();
            }
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public long getRowsRead() {
        return rowsRead;
    }
}
