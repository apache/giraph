package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.HadoopVertex;

/**
 * Just a simple Vertex compute implementation that executes 3 supersteps, then
 * finishes.
 * @author aching
 *
 */
public class TestSuperstepVertex extends
    HadoopVertex<LongWritable, IntWritable, Float, IntWritable> {
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getSuperstep() > 3) {
            voteToHalt();
        }
    }
}
