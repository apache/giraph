package com.yahoo.hadoop_bsp.lib;

import org.apache.hadoop.io.LongWritable;

import com.yahoo.hadoop_bsp.Aggregator;

/**
 * Aggregator for summing up values.
 *
 */

public class LongSumAggregator implements Aggregator<LongWritable> {

  private long sum = 0;

  public void aggregate(long value) {
      sum += value;
  }

  public void aggregate(LongWritable value) {
      sum += value.get();
  }

  public void setAggregatedValue(LongWritable value) {
      sum = value.get();
  }

  public LongWritable getAggregatedValue() {
      return new LongWritable(sum);
  }

  public LongWritable createAggregatedValue() {
      return new LongWritable();
  }
  
}
