package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.Form;

public class MongoExportJobConfiguration {
    @Form
    private MongoExportForm table = new MongoExportForm();

    public MongoExportJobConfiguration() {
    }

    public MongoExportForm getTable() {
        return table;
    }
    
}
