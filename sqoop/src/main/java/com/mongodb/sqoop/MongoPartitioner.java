package com.mongodb.sqoop;

import com.mongodb.sqoop.configuration.MongoConnectionConfiguration;
import com.mongodb.sqoop.configuration.MongoImportJobConfiguration;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.Partitioner;
import org.apache.sqoop.job.etl.PartitionerContext;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class MongoPartitioner extends Partitioner<MongoConnectionConfiguration, MongoImportJobConfiguration> {

    @Override
    public List<Partition> getPartitions(final PartitionerContext context, final MongoConnectionConfiguration mongoConnectionConfiguration,
                                         final MongoImportJobConfiguration jobConfiguration) {
        List<Partition> partitions;
        long maxPartitions = context.getMaxPartitions();
        String type = context.getString(MongoConnector.PARTITION_FIELD_TYPE);
        String fieldName = context.getString(MongoConnector.PARTITION_FIELD);
        if (Integer.class.getName().equals(type)) {
            partitions = buildPartitions(fieldName, maxPartitions,
                                         context.getInt(MongoConnector.PARTITION_MIN_VALUE, Integer.MIN_VALUE),
                                         context.getInt(MongoConnector.PARTITION_MAX_VALUE, Integer.MAX_VALUE));
        } else if (Long.class.getName().equals(type)) {
            partitions = buildPartitions(fieldName, maxPartitions,
                                         context.getLong(MongoConnector.PARTITION_MIN_VALUE, Long.MIN_VALUE),
                                         context.getLong(MongoConnector.PARTITION_MAX_VALUE, Long.MAX_VALUE));
        } else if (Date.class.getName().equals(type)) {
            String min = context.getString(MongoConnector.PARTITION_MIN_VALUE, null);
            String max = context.getString(MongoConnector.PARTITION_MAX_VALUE, null);
            try {
                Date minDate = min == null ? new Date(Long.MIN_VALUE) : DateFormat.getDateTimeInstance().parse(min);
                Date maxDate = max == null ? new Date(Long.MAX_VALUE) : DateFormat.getDateTimeInstance().parse(max);
                partitions = buildPartitions(fieldName, maxPartitions, minDate, maxDate);
            } catch (ParseException e) {
                throw new SqoopException(MongoConnectorError.MONGO_CONNECTOR_0001, e);
            }
        } else if (Double.class.getName().equals(type)) {
            String min = context.getString(MongoConnector.PARTITION_MIN_VALUE, null);
            String max = context.getString(MongoConnector.PARTITION_MAX_VALUE, null);
            Double minDouble = min == null ? Double.MIN_VALUE : Double.valueOf(min);
            Double maxDouble = max == null ? Double.MAX_VALUE : Double.valueOf(max);
            partitions = buildPartitions(fieldName, maxPartitions, minDouble, maxDouble);
        } else if (Float.class.getName().equals(type)) {
            String min = context.getString(MongoConnector.PARTITION_MIN_VALUE, null);
            String max = context.getString(MongoConnector.PARTITION_MAX_VALUE, null);
            Float minFloat = min == null ? Float.MIN_VALUE : Float.valueOf(min);
            Float maxFloat = max == null ? Float.MAX_VALUE : Float.valueOf(max);
            partitions = buildPartitions(fieldName, maxPartitions, minFloat, maxFloat);
        } else {
            partitions = Arrays.<Partition>asList(new MongoPartition(fieldName, context.getString(MongoConnector.PARTITION_MIN_VALUE, null),
                                                          context.getString(MongoConnector.PARTITION_MAX_VALUE, null)));
        }
        return partitions;
    }

    private List<Partition> buildPartitions(final String name, final long maxPartitions, final Date min, final Date max) {
        List<Partition> partitions = new ArrayList<Partition>();
        Calendar start = Calendar.getInstance();
        start.setTime(min);
        if (maxPartitions > 0) {
            int range = (int) ((max.getTime() - min.getTime()) / maxPartitions);
            for (int i = 0; i < maxPartitions - 1; i++) {
                Date minValue = start.getTime();
                start.add(Calendar.MILLISECOND, range);
                Date maxValue = start.getTime();
                partitions.add(new MongoPartition(name, minValue, maxValue));
            }
        }
        partitions.add(new MongoPartition(name, start.getTime(), max));

        return partitions;
    }

    private List<Partition> buildPartitions(final String name, final long maxPartitions, final Long min, final Long max) {
        List<Partition> partitions = new ArrayList<Partition>();
        Long start = min;
        if (maxPartitions > 0) {
            Long range = (max - min) / maxPartitions;
            for (int i = 0; i < maxPartitions - 1; i++) {
                partitions.add(new MongoPartition(name, start, start + range));
                start += range;
            }
        }
        partitions.add(new MongoPartition(name, start, max));

        return partitions;
    }
    
    private List<Partition> buildPartitions(final String name, final long maxPartitions, final Double min, final Double max) {
        List<Partition> partitions = new ArrayList<Partition>();
        Double start = min;
        if (maxPartitions > 0) {
            Double range = (max - min) / maxPartitions;
            for (int i = 0; i < maxPartitions - 1; i++) {
                partitions.add(new MongoPartition(name, start, start + range));
                start += range;
            }
        }
        partitions.add(new MongoPartition(name, start, max));

        return partitions;
    }
    
    private List<Partition> buildPartitions(final String name, final long maxPartitions, final Float min, final Float max) {
        List<Partition> partitions = new ArrayList<Partition>();
        Float start = min;
        if (maxPartitions > 0) {
            Float range = (max - min) / maxPartitions;
            for (int i = 0; i < maxPartitions - 1; i++) {
                partitions.add(new MongoPartition(name, start, start + range));
                start += range;
            }
        }
        partitions.add(new MongoPartition(name, start, max));

        return partitions;
    }

    private List<Partition> buildPartitions(final String name, final long maxPartitions, final int min, final int max) {
        List<Partition> partitions = new ArrayList<Partition>();
        long start = min;
        if (maxPartitions > 0) {
            int range = (int) ((max - min) / maxPartitions);
            for (int i = 0; i < maxPartitions - 1; i++) {
                partitions.add(new MongoPartition(name, start, start + range));
                start += range;
            }
        }
        partitions.add(new MongoPartition(name, start, max));

        return partitions;
    }
}
