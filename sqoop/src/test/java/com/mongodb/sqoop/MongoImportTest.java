package com.mongodb.sqoop;

import org.junit.Test;

public class MongoImportTest extends BaseSqoopTest {
    @Test
    public void importMongo() {
        SqoopJob job = new SqoopJob();
        
        job.run();
    }
}
