package org.apache.giraph.examples;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for getting max value.
 *
 **/

public class MaxAggregator implements Aggregator<DoubleWritable> {

  private double max = Double.MIN_VALUE;

  public void aggregate(DoubleWritable value) {
      double val = value.get();
      if (val > max) {
          max = val;
      }
  }

  public void setAggregatedValue(DoubleWritable value) {
      max = value.get();
  }

  public DoubleWritable getAggregatedValue() {
      return new DoubleWritable(max);
  }

  public DoubleWritable createAggregatedValue() {
      return new DoubleWritable();
  }
  
}
