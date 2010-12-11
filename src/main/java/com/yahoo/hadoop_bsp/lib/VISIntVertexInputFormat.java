package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * This VertexIntInputFormat extends VISInputFormat
 * for the vertex index type of IntWritable.
 *
 */
public class VISIntVertexInputFormat extends VISVertexInputFormat<IntWritable> implements 
	VertexInputFormat<IntWritable, DoubleWritable, Float> {

	public VertexReader<IntWritable, DoubleWritable, Float> createVertexReader(
		    InputSplit split, TaskAttemptContext context) 
		    throws IOException {
		  return new VISIntVertexReader();
	}

}
