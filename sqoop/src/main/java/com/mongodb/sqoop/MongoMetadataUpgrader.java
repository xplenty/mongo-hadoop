package com.mongodb.sqoop;

import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJobForms;

public class MongoMetadataUpgrader extends MetadataUpgrader {
    @Override
    public void upgrade(final MConnectionForms original, final MConnectionForms upgradeTarget) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void upgrade(final MJobForms original, final MJobForms upgradeTarget) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }
}
