package com.mongodb.sqoop;

import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoExportJobConfiguration;
import org.apache.sqoop.job.etl.Loader;
import org.apache.sqoop.job.etl.LoaderContext;

public class MongoLoader extends Loader<MongoConnectionConfiguration, MongoExportJobConfiguration> {
    @Override
    public void load(final LoaderContext context, final MongoConnectionConfiguration mongoConnectionConfiguration,
                     final MongoExportJobConfiguration mongoExportJobConfiguration) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
