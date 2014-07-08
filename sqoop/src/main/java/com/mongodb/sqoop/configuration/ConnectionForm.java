package com.mongodb.sqoop.configuration;

import org.apache.sqoop.model.FormClass;
import org.apache.sqoop.model.Input;

@FormClass
public class ConnectionForm {
    @Input(size = 200) private String uri;

    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }
}
