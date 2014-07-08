package com.mongodb.sqoop;

import org.apache.sqoop.common.ErrorCode;

public enum MongoConnectorError implements ErrorCode {
    MONGO_CONNECTOR_0000("Could not connect to server"),
    
    MONGO_CONNECTOR_0001("Could not find the min and max values for partitioning"),
    
    MONGO_CONNECTOR_0002("Unknown partition field type");

    private final String message;

    MongoConnectorError(final String message) {
        this.message = message;
    }

    @Override
    public String getCode() {
        return name();
    }

    @Override
    public String getMessage() {
        return message;
    }
}
