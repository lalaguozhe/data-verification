package com.dinaping.hive;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hadoop on 14-4-15.
 */
public class MapValueData implements Writable {
    private String tableName;
    private int partialCount;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getPartialCount() {
        return partialCount;
    }

    public void setPartialCount(int partialCount) {
        this.partialCount = partialCount;
    }

    @Override
    public void write(DataOutput output) throws IOException{
        WritableUtils.writeString(output, tableName);
        WritableUtils.writeVInt(output, partialCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tableName = WritableUtils.readString(in);
        partialCount = WritableUtils.readVInt(in);
    }

}
