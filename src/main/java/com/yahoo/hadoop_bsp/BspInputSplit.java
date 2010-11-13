package com.yahoo.hadoop_bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This InputSplit will not give any ordering or location data.  It is meant to
 * do almost nothing.
 * @author aching
 *
 */
public class BspInputSplit extends InputSplit implements Writable {
	/** Arbitrary name */
	private String m_name = new String();
	
	public BspInputSplit() {
		m_name = this.getName();
	}

	public BspInputSplit(String name) {
		m_name = name;
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
	    m_name = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException {
	    Text.writeString(out, m_name);
	}
	
	public String getName() {
		return m_name;
	}
}
