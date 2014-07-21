package com.mongodb.sqoop;

import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.job.etl.Destroyer;
import org.apache.sqoop.job.etl.DestroyerContext;

public class MongoDestroyer extends Destroyer<MongoConnectionConfiguration, MongoImportJobConfiguration> {
    @Override
    public void destroy(final DestroyerContext context, final MongoConnectionConfiguration mongoConnectionConfiguration,
                        final MongoImportJobConfiguration mongoImportJobConfiguration) {
    }
}
