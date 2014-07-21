package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Form;

@ConfigurationClass
public class MongoExportJobConfiguration {
    @Form
    private MongoExportForm exportForm = new MongoExportForm();

    public MongoExportJobConfiguration() {
    }

    public MongoExportForm getExportForm() {
        return exportForm;
    }
    
}
