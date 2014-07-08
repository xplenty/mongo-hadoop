package com.mongodb.sqoop;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Locale;

public class MongoConnectorTest {

    @Test
    public void testGetVersion() throws Exception {
        Assume.assumeFalse(System.getProperty("project.version") == null);
        Assert.assertEquals(System.getProperty("project.version"), new MongoConnector().getVersion());
    }
    
    @Test
    public void testGetBundle() {
        Assert.assertNotNull(new MongoConnector().getBundle(Locale.getDefault()));
    }
}