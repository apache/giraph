package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.OutEdgeIterator;
import com.yahoo.hadoop_bsp.VertexWriter;

/**
 * Writes out VIS graph in text format.
 *
 */
public class VISIntVertexWriter extends TextOutputFormat<NullWritable, Text> implements 
	VertexWriter<IntWritable, DoubleWritable, Float> {
	/** Logger */
    private static final Logger LOG = Logger.getLogger(VISVertexWriter.class);
	
	public <KEYOUT, VALUEOUT> void write(
            TaskInputOutputContext<Object, Object,
                                   KEYOUT, VALUEOUT> context,
            IntWritable vertexId, 
			      DoubleWritable vertexValue,
			      OutEdgeIterator<IntWritable, Float> destEdgeIt) 
	          throws IOException, InterruptedException {
           
      StringBuilder sb = new StringBuilder();
      sb.append(vertexId.toString());
      sb.append('\t');
      sb.append(vertexValue.toString());
      context.write((KEYOUT)new Text(sb.toString()), (VALUEOUT)null);
	}

  public void close(TaskAttemptContext context
                      ) throws IOException, InterruptedException {
  }
}
