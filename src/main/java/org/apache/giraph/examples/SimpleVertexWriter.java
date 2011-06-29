package org.apache.giraph.examples;

import java.io.IOException;
import java.util.SortedMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.VertexWriter;

/**
 * Simple vertex writer.
 */
public class SimpleVertexWriter implements
         VertexWriter<LongWritable, IntWritable, FloatWritable> {

    @Override
    public <KEYOUT, VALUEOUT> void write(
            TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context,
            LongWritable vertexId,
            IntWritable vertexValue,
            SortedMap<LongWritable, Edge<LongWritable, FloatWritable>> destEdgeMap)
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
