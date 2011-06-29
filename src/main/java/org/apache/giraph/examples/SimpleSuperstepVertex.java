package org.apache.giraph.examples;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.giraph.graph.Vertex;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 */
public class SimpleSuperstepVertex extends
    Vertex<LongWritable, IntWritable, FloatWritable, IntWritable> {
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getSuperstep() > 3) {
            voteToHalt();
        }
    }

    public IntWritable createMsgValue() {
        return new IntWritable(0);
    }
}
