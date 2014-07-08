package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.Form;

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
