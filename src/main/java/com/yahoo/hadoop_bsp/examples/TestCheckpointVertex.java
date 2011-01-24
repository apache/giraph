package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.HadoopVertex;
import com.yahoo.hadoop_bsp.OutEdgeIterator;
import com.yahoo.hadoop_bsp.lib.LongSumAggregator;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that checkpoint restarting works.
 *
 * @author aching
 */
public class TestCheckpointVertex extends
    HadoopVertex<LongWritable, IntWritable, FloatWritable, FloatWritable> {
    /** Simple test to keep adding the vertex ids together. */
    private static LongSumAggregator sumAggregator = null;

    /** Setup the sum aggregator for use in this application */
    private static void registerAggregators() {
        sumAggregator = new LongSumAggregator();
        registerAggregator(LongSumAggregator.class.getName(), sumAggregator);
        sumAggregator.setAggregatedValue(new LongWritable(0));
    }

    @Override
    public void preApplication() {
        registerAggregators();
    }

    @Override
    public void preSuperstep() {
        useAggregator(LongSumAggregator.class.getName());
    }

    public void compute(Iterator<FloatWritable> msgIterator) {
        if (getSuperstep() > 6) {
            voteToHalt();
        }
        sumAggregator.aggregate(getVertexId().get());
        int vertexValue = getVertexValue().get();
        float msgValue = 0.0f;
        while (msgIterator.hasNext()) {
            msgValue += msgIterator.next().get();
        }
        setVertexValue(new IntWritable(vertexValue + (int) msgValue));
        OutEdgeIterator<LongWritable, FloatWritable> it = getOutEdgeIterator();
        while (it.hasNext()) {
            Map.Entry<LongWritable, FloatWritable> entry = it.next();
            float edgeValue = entry.getValue().get();
            entry.getValue().set(edgeValue + (float) vertexValue);
            sendMsg(entry.getKey(), new FloatWritable(edgeValue));
        }
    }

    public FloatWritable createMsgValue() {
        return new FloatWritable(0);
    }
}
