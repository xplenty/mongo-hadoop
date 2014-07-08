package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.Form;

public class MongoImportJobConfiguration {
    @Form
    private MongoImportForm collectionForm = new MongoImportForm();

    public MongoImportJobConfiguration() {
    }

    public MongoImportForm getCollectionForm() {
        return collectionForm;
    }
}
