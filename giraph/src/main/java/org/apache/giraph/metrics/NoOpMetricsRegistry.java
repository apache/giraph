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

package org.apache.giraph.metrics;

import com.google.common.collect.Maps;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.NoOpCounter;
import com.yammer.metrics.core.NoOpGuage;
import com.yammer.metrics.core.NoOpHistogram;
import com.yammer.metrics.core.NoOpMeter;
import com.yammer.metrics.core.NoOpTimer;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import com.yammer.metrics.core.Timer;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An empty MetricsRegistry, used to easily turn off all metrics without
 * affecting client code.
 */
public class NoOpMetricsRegistry extends MetricsRegistry {
  @Override
  public <T> Gauge<T> newGauge(Class<?> klass, String name, Gauge<T> metric) {
    return NoOpGuage.INSTANCE;
  }

  @Override
  public <T> Gauge<T> newGauge(Class<?> klass, String name, String scope,
                               Gauge<T> metric) {
    return NoOpGuage.INSTANCE;
  }

  @Override
  public <T> Gauge<T> newGauge(MetricName metricName, Gauge<T> metric) {
    return NoOpGuage.INSTANCE;
  }

  @Override
  public Counter newCounter(Class<?> klass, String name) {
    return NoOpCounter.INSTANCE;
  }

  @Override
  public Counter newCounter(Class<?> klass, String name, String scope) {
    return NoOpCounter.INSTANCE;
  }

  @Override
  public Counter newCounter(MetricName metricName) {
    return NoOpCounter.INSTANCE;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, boolean biased) {
    return NoOpHistogram.INSTANCE;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, String scope,
                                boolean biased) {
    return NoOpHistogram.INSTANCE;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name) {
    return NoOpHistogram.INSTANCE;
  }

  @Override
  public Histogram newHistogram(Class<?> klass, String name, String scope) {
    return NoOpHistogram.INSTANCE;
  }

  @Override
  public Histogram newHistogram(MetricName metricName, boolean biased) {
    return NoOpHistogram.INSTANCE;
  }

  @Override
  public Meter newMeter(Class<?> klass, String name, String eventType,
                        TimeUnit unit) {
    return NoOpMeter.INSTANCE;
  }

  @Override
  public Meter newMeter(
    Class<?> klass, String name, String scope, String eventType, TimeUnit unit
  ) {
    return NoOpMeter.INSTANCE;
  }

  @Override
  public Meter newMeter(MetricName metricName, String eventType,
                        TimeUnit unit) {
    return NoOpMeter.INSTANCE;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name) {
    return NoOpTimer.INSTANCE;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, TimeUnit durationUnit,
                        TimeUnit rateUnit) {
    return NoOpTimer.INSTANCE;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, String scope) {
    return NoOpTimer.INSTANCE;
  }

  @Override
  public Timer newTimer(Class<?> klass, String name, String scope,
                        TimeUnit durationUnit, TimeUnit rateUnit
  ) {
    return NoOpTimer.INSTANCE;
  }

  @Override
  public Timer newTimer(MetricName metricName, TimeUnit durationUnit,
                        TimeUnit rateUnit) {
    return NoOpTimer.INSTANCE;
  }

  @Override
  public Map<MetricName, Metric> allMetrics() {
    return Maps.newHashMap();
  }

  @Override
  public SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics() {
    return Maps.newTreeMap();
  }

  @Override
  public SortedMap<String, SortedMap<MetricName, Metric>>
  groupedMetrics(MetricPredicate predicate) {
    return Maps.newTreeMap();
  }

  @Override
  public void shutdown() { }

  @Override
  public ScheduledExecutorService newScheduledThreadPool(int poolSize,
                                                         String name) {
    return null;
  }

  @Override
  public void removeMetric(Class<?> klass, String name) { }

  @Override
  public void removeMetric(Class<?> klass, String name, String scope) { }

  @Override
  public void removeMetric(MetricName name) { }

  @Override
  public void addListener(MetricsRegistryListener listener) { }

  @Override
  public void removeListener(MetricsRegistryListener listener) { }

  @Override
  protected ConcurrentMap<MetricName, Metric> newMetricsMap() {
    return Maps.newConcurrentMap();
  }
}
