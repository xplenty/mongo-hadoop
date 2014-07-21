package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.FormClass;
import org.apache.sqoop.model.Input;

@FormClass
public class MongoImportForm {
    @Input
    private String collection;
    @Input
    private String database;
    @Input
    private String partitionField;
    @Input
    private String uri;

    public String getCollection() {
        return collection;
    }

    public void setCollection(final String collection) {
        this.collection = collection;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(final String database) {
        this.database = database;
    }

    public void setPartitionField(final String partitionField) {
        this.partitionField = partitionField;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }
}
