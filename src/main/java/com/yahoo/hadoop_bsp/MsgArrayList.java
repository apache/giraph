package com.yahoo.hadoop_bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * A Writable for ListArray containing instances of a class.
 */
public class MsgArrayList<M extends Writable> extends ArrayList<M>
          implements Writable, Configurable {

    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1L;
    private Configuration conf;

    MsgArrayList() {
        super();
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    private M msgInstance() {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Writable> msgValueClass =
                (Class<Writable>) conf.getClass("bsp.msgValueClass", Writable.class);
            @SuppressWarnings("unchecked")
            M newInstance =
                    (M)ReflectionUtils.newInstance(msgValueClass, conf);
            return newInstance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int numValues = in.readInt();            // read number of values
        ensureCapacity(numValues);
        for (int i = 0; i < numValues; i++) {
            M value = msgInstance();
            value.readFields(in);                // read a value
            add(value);                          // store it in values
        }
    }

    public void write(DataOutput out) throws IOException {
        int numValues = size();
        out.writeInt(numValues);                 // write number of values
        for (int i = 0; i < numValues; i++) {
            get(i).write(out);
        }
    }
}
