package com.yahoo.hadoop_bsp.lib;

import org.apache.hadoop.io.DoubleWritable;

import com.yahoo.hadoop_bsp.Aggregator;

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
