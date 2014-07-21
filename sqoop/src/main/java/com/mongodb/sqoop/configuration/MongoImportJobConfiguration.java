package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Form;

@ConfigurationClass
public class MongoImportJobConfiguration {
    @Form
    private MongoImportForm importForm = new MongoImportForm();

    public MongoImportJobConfiguration() {
    }

    public MongoImportForm getImportForm() {
        return importForm;
    }
}
