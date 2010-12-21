package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.yahoo.hadoop_bsp.VertexInputFormat;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * This VertexTextInputFormat extends VISInputFormat
 * for the vertex index type of Text.
 *
 */
public class VISTextVertexInputFormat
	extends VISVertexInputFormat<Text, DoubleWritable, Float>
	implements VertexInputFormat<Text, DoubleWritable, Float> {

	public VertexReader<Text, DoubleWritable, Float> createVertexReader(
		    InputSplit split, TaskAttemptContext context) 
		    throws IOException {
          VISTextVertexReader reader = new VISTextVertexReader();
          reader.setConf(context.getConfiguration());
		  return reader;
	}

}
