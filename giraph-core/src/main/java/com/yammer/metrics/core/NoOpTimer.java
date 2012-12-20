/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yammer.metrics.core;

import com.yammer.metrics.stats.Snapshot;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * An empty Metrics' Timer implementation that does nothing.
 */
public class NoOpTimer extends Timer {
  /** singleton instance for everyone to use */
  public static final NoOpTimer INSTANCE = new NoOpTimer();

  /** do not instantiate */
  private NoOpTimer() {
    super(new NoOpExecutorService(), TimeUnit.SECONDS, TimeUnit.SECONDS);
  }

  @Override
  public void clear() { }

  @Override
  public void update(long duration, TimeUnit unit) { }

  @Override
  public <T> T time(Callable<T> event) throws Exception { return event.call(); }

  @Override
  public long count() { return 0; }

  @Override
  public double fifteenMinuteRate() { return 0.0; }

  @Override
  public double fiveMinuteRate() { return 0.0; }

  @Override
  public double meanRate() { return 0.0; }

  @Override
  public double oneMinuteRate() { return 0.0; }

  @Override
  public double max() { return 0.0; }

  @Override
  public double min() { return 0.0; }

  @Override
  public double mean() { return 0.0; }

  @Override
  public double stdDev() { return 0.0; }

  @Override
  public double sum() { return 0.0; }

  @Override
  public Snapshot getSnapshot() { return new Snapshot(new double[0]); }

  @Override
  public String eventType() { return ""; }

  @Override
  public void stop() { }

  @Override
  public <T> void processWith(MetricProcessor<T> processor, MetricName name,
                              T context)
    throws Exception { }
}
