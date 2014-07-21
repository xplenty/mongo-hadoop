package com.mongodb.sqoop;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.sqoop.job.etl.Partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MongoPartition extends Partition {
    private final String name;
    private Object min;
    private Object max;
    private DBObject condition;

    public MongoPartition(final String name, final Object min, final Object max) {
        this.name = name;
        this.min = min;
        this.max = max;
        DBObject minCheck = new BasicDBObject(name, new BasicDBObject("$gte", min));
        DBObject maxCheck = new BasicDBObject(name, new BasicDBObject("$lt", max));
        BasicDBList list = new BasicDBList();
        list.add(minCheck);
        list.add(maxCheck);
        condition = new BasicDBObject("$and", list);
    }

    public DBObject getCondition() {
        return condition;
    }

    public String getName() {
        return name;
    }

    public Object getMin() {
        return min;
    }

    public Object getMax() {
        return max;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(final DataInput in) throws IOException {
        condition = (DBObject) JSON.parse(in.readUTF());
        List<DBObject> list = (List<DBObject>) condition.get("$and");
        DBObject dbObject = list.get(0);
        String name = dbObject.keySet().iterator().next();
        min = ((DBObject) dbObject.get(name)).get("$gte");
        dbObject = list.get(1);
        max = ((DBObject) dbObject.get(name)).get("$lt");
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeUTF(JSON.serialize(condition));
    }

    @Override
    public String toString() {
        return condition.toString();
    }
}
