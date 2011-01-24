package com.yahoo.hadoop_bsp.examples;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.yahoo.hadoop_bsp.OutEdgeIterator;
import com.yahoo.hadoop_bsp.VertexWriter;

/**
 * Writes out VIS graph in text format.
 *
 */
public class TestVertexWriter implements
         VertexWriter<LongWritable, IntWritable, FloatWritable> {

    public <KEYOUT, VALUEOUT> void write(
        TaskInputOutputContext<Object, Object, KEYOUT, VALUEOUT> context,
        LongWritable vertexId,
        IntWritable vertexValue,
        OutEdgeIterator<LongWritable, FloatWritable> destEdgeIt)
        throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(vertexId.toString());
        sb.append('\t');
        sb.append(vertexValue.toString());
        @SuppressWarnings("unchecked")
        KEYOUT keyout = (KEYOUT) new Text(sb.toString());
        context.write(keyout, (VALUEOUT) null);
    }

    public void close(TaskAttemptContext context)
        throws IOException, InterruptedException {
    }
}
