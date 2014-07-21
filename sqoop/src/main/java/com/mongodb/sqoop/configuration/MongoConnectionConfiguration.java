package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.Form;

@ConfigurationClass
public class MongoConnectionConfiguration {
    @Form
    private ConnectionForm connectionForm;

    public MongoConnectionConfiguration() {
        connectionForm = new ConnectionForm();
    }

    public ConnectionForm getConnectionForm() {
        return connectionForm;
    }

    public void setConnectionForm(final ConnectionForm connectionForm) {
        this.connectionForm = connectionForm;
    }
}
