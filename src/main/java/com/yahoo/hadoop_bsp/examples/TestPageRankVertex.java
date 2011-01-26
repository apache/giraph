package com.yahoo.hadoop_bsp.examples;

import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.HadoopVertex;
import com.yahoo.hadoop_bsp.lib.MaxAggregator;
import com.yahoo.hadoop_bsp.lib.MinAggregator;
import com.yahoo.hadoop_bsp.lib.LongSumAggregator;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 * @author aching
 *
 */
public class TestPageRankVertex extends
    HadoopVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    private static LongSumAggregator sumAggreg = null;
    private static MinAggregator minAggreg = null;
    private static MaxAggregator maxAggreg = null;
    /** Logger */
    private static final Logger LOG = Logger.getLogger(TestPageRankVertex.class);

    private static void registerAggregators() {
        sumAggreg = new LongSumAggregator();
        minAggreg = new MinAggregator();
        maxAggreg = new MaxAggregator();
        registerAggregator("sum", sumAggreg);
        registerAggregator("min", minAggreg);
        registerAggregator("max", maxAggreg);
    }

    @Override
    public void preApplication() {
        if (sumAggreg == null) {
            registerAggregators();
        }
    }

    @Override
    public void preSuperstep() {
        if (getSuperstep() >= 2) {
            LOG.info("aggregatedNumVertices=" +
                    sumAggreg.getAggregatedValue() +
                    " NumVertices=" + getNumVertices());
            if (sumAggreg.getAggregatedValue().get() != getNumVertices()) {
                throw new RuntimeException("wrong value of SumAggreg: " +
                        sumAggreg.getAggregatedValue() + ", should be: " +
                        getNumVertices());
            }
            DoubleWritable maxPagerank =
                    (DoubleWritable)maxAggreg.getAggregatedValue();
            LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
            DoubleWritable minPagerank =
                    (DoubleWritable)minAggreg.getAggregatedValue();
            LOG.info("aggregatedMinPageRank=" + minPagerank.get());
        }
        useAggregator("sum");
        useAggregator("min");
        useAggregator("max");
        sumAggreg.setAggregatedValue(new LongWritable(0L));
    }

    public void compute(Iterator<DoubleWritable> msgIterator) {
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += msgIterator.next().get();
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            setVertexValue(vertexValue);
            maxAggreg.aggregate(vertexValue);
            minAggreg.aggregate(vertexValue);
            sumAggreg.aggregate(1L);
            LOG.info(getVertexId() + ": PageRank=" + vertexValue +
                    " max=" + maxAggreg.getAggregatedValue() +
                    " min=" + minAggreg.getAggregatedValue());

            if (getSuperstep() < 30) {
                long edges = getOutEdgeIterator().size();
                sentMsgToAllEdges(
                    new DoubleWritable(getVertexValue().get() / edges));
            } else {
                voteToHalt();
            }
        }
    }

    public DoubleWritable createMsgValue() {
        return new DoubleWritable(0f);
    }
}
