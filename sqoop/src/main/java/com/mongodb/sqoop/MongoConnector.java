package com.mongodb.sqoop;

import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoExportJobConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.VersionAnnotation;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.model.MJob.Type;
import org.apache.sqoop.validation.Validator;

import java.util.Locale;
import java.util.ResourceBundle;

public class MongoConnector extends SqoopConnector {
    public static final String RESOURCE_BUNDLE_NAME = "mongo-connector-resources";
    public static final String PARTITION_FIELD = "mongo.sqoop.partition.field";
    public static final String PARTITION_FIELD_TYPE = "mongo.sqoop.partition.field.type";
    public static final String PARTITION_MAX_VALUE = "mongo.sqoop.partition.max.value";
    public static final String PARTITION_MIN_VALUE = "mongo.sqoop.partition.min.value";

    private static String version;

    @Override
    public String getVersion() {
        if (version == null) {
            Package aPackage = Package.getPackage("com.mongodb.sqoop");
            VersionAnnotation annotation = aPackage.getAnnotation(VersionAnnotation.class);
            version = annotation.version();
        }
        return version;
    }

    @Override
    public ResourceBundle getBundle(final Locale locale) {
        return ResourceBundle.getBundle(RESOURCE_BUNDLE_NAME, locale);
    }

    @Override
    public Class getConnectionConfigurationClass() {
        return MongoConnectionConfiguration.class;
    }

    @Override
    public Class getJobConfigurationClass(final Type type) {
        switch (type) {
            case IMPORT:
                return MongoImportJobConfiguration.class;
            case EXPORT:
                return MongoExportJobConfiguration.class;
            default:
                return null;
        }
    }

    @Override
    public Importer getImporter() {
        return new Importer(MongoImportInitializer.class, MongoPartitioner.class, MongoExtractor.class,
                            MongoDestroyer.class);
    }

    @Override
    public Exporter getExporter() {
        return new Exporter(MongoImportInitializer.class, MongoLoader.class, MongoDestroyer.class);
    }

    @Override
    public Validator getValidator() {
        return new MongoValidator();
    }

    @Override
    public MetadataUpgrader getMetadataUpgrader() {
        return new MongoMetadataUpgrader();
    }

}
