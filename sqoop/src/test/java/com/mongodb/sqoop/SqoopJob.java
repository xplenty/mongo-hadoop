package com.mongodb.sqoop;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.testutils.BaseHadoopTest;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJob.Type;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class SqoopJob {
    private static final Logger LOG = LoggerFactory.getLogger(SqoopJob.class);

    private SqoopClient client;
    private MongoClientURI uri;

    public void run() {
        client = new SqoopClient("http://localhost:12000/sqoop/");

        //Dummy connection object
        MConnection newCon = client.newConnection("mongo-connector");

        //Get connection and framework forms. Set name for connection
        MConnectionForms conForms = newCon.getConnectorPart();
        MConnectionForms frameworkForms = newCon.getFrameworkPart();
        newCon.setName("MyConnection");

        //Set connection forms values
        uri = BaseHadoopTest.authCheck(new MongoClientURIBuilder()
                                           .collection("mongo_hadoop", "sqoop"))
                            .build();
        conForms.getStringInput("connectionForm.uri").setValue(uri.toString());

        Status status = client.createConnection(newCon);
        if (status.canProceed()) {
            LOG.info("Created. New Connection ID : " + newCon.getPersistenceId());
        } else {
            LOG.info("Check for status and forms error ");
        }

        MJob job = createJob(newCon);
        watchJob(job);
    }

    private MJob createJob(final MConnection connection) {
        MJob job = client.newJob(connection.getPersistenceId(), Type.IMPORT);
        MJobForms connectorForm = job.getConnectorPart();
        MJobForms frameworkForm = job.getFrameworkPart();

        job.setName("ImportJob");
        connectorForm.getStringInput("importForm.collection").setValue(uri.getCollection());
        connectorForm.getStringInput("importForm.database").setValue(uri.getDatabase());
        connectorForm.getStringInput("importForm.partitionField").setValue("age");
        connectorForm.getStringInput("importForm.uri").setValue(uri.toString());

        frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
        frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE"); //Other option: SEQUENCE_FILE
        frameworkForm.getStringInput("output.outputDirectory").setValue("/output");

        //Job resources
        frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
        frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

        Status status = client.createJob(job);
        if (status.canProceed()) {
            LOG.info("New Job ID: " + job.getPersistenceId());
        } else {
            MSubmission submissionStatus = client.getSubmissionStatus(job.getPersistenceId());
            LOG.info("Check for status and forms error ");
        }

        return job;
    }

    private void watchJob(final MJob job) {
        //Job submission start
        MSubmission submission = client.startSubmission(job.getPersistenceId());
        LOG.info("Status : " + submission.getStatus());
        if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            LOG.info("Progress : " + format("%.2f %%", submission.getProgress() * 100));
        }
        LOG.info("Hadoop job id :" + submission.getExternalId());
        LOG.info("Job link : " + submission.getExternalLink());
        Counters counters = submission.getCounters();
        if (counters != null) {
            StringBuilder builder = new StringBuilder("Counters:");
            for (CounterGroup group : counters) {
                builder.append("\t");
                LOG.info(group.getName());
                for (Counter counter : group) {
                    builder.append(format("\t\t%s: %d", counter.getName(), counter.getValue()));
                }
            }
            LOG.info(builder.toString());
        }
        if (submission.getExceptionInfo() != null) {
            throw new SqoopException(MongoConnectorError.MONGO_CONNECTOR_0003, submission.getExceptionInfo());
        }


        //Check job status
        if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
            LOG.info("Progress : " + format("%.2f %%", submission.getProgress() * 100));
        }

        //Stop a running job
        client.stopSubmission(job.getPersistenceId());
    }
}
