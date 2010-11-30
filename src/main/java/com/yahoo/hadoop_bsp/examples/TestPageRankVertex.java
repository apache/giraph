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
    HadoopVertex<LongWritable, DoubleWritable, Float, Double> {
    public void compute(Iterator<Double> msgIterator) {
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next();
            }
            setVertexValue(
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum));
            
            if (getSuperstep() < 30) {
                long edges = getOutEdgeIterator().size();
                sentMsgToAllEdges(new Double(getVertexValue().get() / edges));
            } else {
                voteToHalt();  
            }
        }
    }
}


