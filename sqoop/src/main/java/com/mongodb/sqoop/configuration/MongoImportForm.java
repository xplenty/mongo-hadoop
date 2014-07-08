package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.FormClass;

@FormClass
public class MongoImportForm {
    private String collection;
    private String database;
    private String partitionField;
    private String url;

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

    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
    }
}
