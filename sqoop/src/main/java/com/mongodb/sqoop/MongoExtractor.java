package com.mongodb.sqoop;

import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoExportJobConfiguration;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.etl.Partition;

public class MongoExtractor extends Extractor<MongoConnectionConfiguration, MongoExportJobConfiguration, Partition> {
    @Override
    public void extract(final ExtractorContext context, final MongoConnectionConfiguration mongoConnectionConfiguration,
                        final MongoExportJobConfiguration mongoExportJobConfiguration, final Partition partition) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public long getRowsRead() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
