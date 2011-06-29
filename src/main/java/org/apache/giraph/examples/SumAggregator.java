package org.apache.giraph.examples;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Aggregator;

/**
 * Aggregator for summing up values.
 *
 */
public class SumAggregator implements Aggregator<DoubleWritable> {

  private double sum = 0;

  public void aggregate(double value) {
      sum += value;
  }

  public void aggregate(DoubleWritable value) {
      sum += value.get();
  }

  public void setAggregatedValue(DoubleWritable value) {
      sum = value.get();
  }

  public DoubleWritable getAggregatedValue() {
      return new DoubleWritable(sum);
  }

  public DoubleWritable createAggregatedValue() {
      return new DoubleWritable();
  }

}
