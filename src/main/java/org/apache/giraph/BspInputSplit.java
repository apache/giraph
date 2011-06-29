package org.apache.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This InputSplit will not give any ordering or location data.
 * It is used internally by BspInputFormat (which determines
 * how many tasks to run the application on).  Users need not use this
 * directly.
 */
public class BspInputSplit extends InputSplit implements Writable {
    /** Number of splits */
    int m_numSplits = -1;
    /** Split index */
    int m_splitIndex = -1;

    public BspInputSplit() {}

    public BspInputSplit(int splitIndex, int numSplits) {
        m_splitIndex = splitIndex;
        m_numSplits = numSplits;
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
        m_splitIndex = in.readInt();
        m_numSplits = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(m_splitIndex);
        out.writeInt(m_numSplits);
    }

    public int getSplitIndex() {
        return m_splitIndex;
    }

    public int getNumSplits() {
        return m_numSplits;
    }
}
