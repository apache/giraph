package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.giraph.BspInputSplit;
import org.apache.giraph.VertexInputFormat;
import org.apache.giraph.VertexReader;

/**
 * This VertexInputFormat is meant for testing/debugging.  It simply generates
 * some vertex data that can be consumed by test applications. *
 */
public class GeneratedVertexInputFormat implements
    VertexInputFormat<LongWritable, IntWritable, FloatWritable> {

    public List<InputSplit> getSplits(Configuration conf, int numSplits)
        throws IOException, InterruptedException {
        /*
         * This is meaningless, the VertexReader will generate all the test
         * data.
         */
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < numSplits; ++i) {
            inputSplitList.add(new BspInputSplit(i, numSplits));
        }
        return inputSplitList;
    }

    public VertexReader<LongWritable, IntWritable, FloatWritable>
            createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new GeneratedVertexReader();
    }

}
