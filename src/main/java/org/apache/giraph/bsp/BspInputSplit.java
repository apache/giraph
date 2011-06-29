package org.apache.giraph.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This InputSplit will not give any ordering or location data.
 * It is used internally by BspInputFormat (which determines
 * how many tasks to run the application on).  Users should not use this
 * directly.
 */
public class BspInputSplit extends InputSplit implements Writable {
    /** Number of splits */
    private int numSplits = -1;
    /** Split index */
    private int splitIndex = -1;

    public BspInputSplit() {}

    public BspInputSplit(int splitIndex, int numSplits) {
        this.splitIndex = splitIndex;
        this.numSplits = numSplits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    public void readFields(DataInput in) throws IOException {
        splitIndex = in.readInt();
        numSplits = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(splitIndex);
        out.writeInt(numSplits);
    }

    public int getSplitIndex() {
        return splitIndex;
    }

    public int getNumSplits() {
        return numSplits;
    }
}
