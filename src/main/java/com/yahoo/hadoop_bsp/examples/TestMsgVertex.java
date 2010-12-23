package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.HadoopVertex;

/**
 * Test whether messages can be sent and received by vertices.
 * @author aching
 *
 */
public class TestMsgVertex extends
    HadoopVertex<LongWritable, IntWritable, Float, IntWritable> {
    public void compute(Iterator<IntWritable> msgIterator) {
        if (getVertexId().equals(new LongWritable(2))) {
            sendMsg(new LongWritable(1), new IntWritable(101));
            sendMsg(new LongWritable(1), new IntWritable(102));
            sendMsg(new LongWritable(1), new IntWritable(103));
        }
        if (!getVertexId().equals(new LongWritable(1))) {
            voteToHalt();
        }
        else {
            /* Check the messages */
            int sum = 0;
            while (msgIterator != null && msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            System.out.println("TestMsgVertex: Received a sum of " + sum +
            " (should have 306)");

            if (sum == 306) {
                voteToHalt();
            }
        }
        if (getSuperstep() > 3) {
            System.err.println("TestMsgVertex: Vertex 1 failed to receive " +
                               "messages in time");
            voteToHalt();
        }
    }
}
