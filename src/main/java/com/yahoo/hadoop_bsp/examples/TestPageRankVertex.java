package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.HadoopVertex;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 * @author aching
 *
 */
public class TestPageRankVertex extends 
    HadoopVertex<LongWritable, DoubleWritable, Float, DoubleWritable> {
    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            setVertexValue(
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum));
            
            if (getSuperstep() < 30) {
                long edges = getOutEdgeIterator().size();
                sentMsgToAllEdges(
                    new DoubleWritable(getVertexValue().get() / edges));
            } else {
                voteToHalt();  
            }
        }
    }
}


