package com.yahoo.hadoop_bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 * A Writable for ListArray containing instances of a class.
 */
public class ArrayListWritable<M extends Writable> extends ArrayList<M>
          implements Writable {
    /** Used for instantiation */
    private final Class<M> refClass;
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1L;

    public ArrayListWritable(Class<M> refClass) {
        super();
        this.refClass = refClass;
    }

    public void readFields(DataInput in) throws IOException {
        int numValues = in.readInt();            // read number of values
        ensureCapacity(numValues);
        try {
            for (int i = 0; i < numValues; i++) {
                M value = refClass.newInstance();
                value.readFields(in);                // read a value
                add(value);                          // store it in values
            }
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
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
