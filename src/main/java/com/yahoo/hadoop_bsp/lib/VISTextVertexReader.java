package com.yahoo.hadoop_bsp.lib;

import org.apache.hadoop.io.DoubleWritable;

import com.yahoo.hadoop_bsp.VertexReader;

/**
 * Used by VISVertexInputFormat to read VIS graph
 *
 */
public class VISTextVertexReader
	      extends VISVertexReader<DoubleWritable, Float> {

	public DoubleWritable createVertexValue() {
		return new DoubleWritable(0.0);
	}

	public Float createEdgeValue() {
		return new Float(0.0);
	}
}
