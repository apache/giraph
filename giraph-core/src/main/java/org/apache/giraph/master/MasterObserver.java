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

package org.apache.giraph.master;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.metrics.AggregatedMetrics;
import org.apache.giraph.partition.PartitionStats;

import java.util.List;

/**
 * Observer for Master.
 * It can implement ContextSettable if it needs to access job counters.
 */
public interface MasterObserver extends ImmutableClassesGiraphConfigurable {
  /**
   * Before application begins.
   */
  void preApplication();

  /**
   * After application ends.
   */
  void postApplication();

  /**
   * If there is an error during the application.
   *
   * @param e Exception that caused failure. May be null.
   */
  void applicationFailed(Exception e);

  /**
   * Before each superstep starts.
   *
   * @param superstep The superstep number
   */
  void preSuperstep(long superstep);

  /**
   * After each superstep ends.
   *
   * @param superstep The superstep number
   */
  void postSuperstep(long superstep);

  /**
   * Called after each superstep with aggregated metrics from workers
   *
   * @param superstep Supsertep number
   * @param aggregatedMetrics Metrics
   * @param partitionStatsList List of partition stats
   */
  void superstepMetricsUpdate(
      long superstep, AggregatedMetrics aggregatedMetrics,
      List<PartitionStats> partitionStatsList);
}
