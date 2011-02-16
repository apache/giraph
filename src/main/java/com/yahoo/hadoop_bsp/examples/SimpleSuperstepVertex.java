package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.HadoopVertex;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 */
public class SimpleSuperstepVertex extends
    HadoopVertex<LongWritable, IntWritable, FloatWritable, IntWritable> {
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getSuperstep() > 3) {
            voteToHalt();
        }
    }

    public IntWritable createMsgValue() {
        return new IntWritable(0);
    }
}
