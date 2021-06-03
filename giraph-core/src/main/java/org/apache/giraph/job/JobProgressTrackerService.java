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

package org.apache.giraph.job;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Implementation of job progress tracker service on job client
 */
public interface JobProgressTrackerService extends JobProgressTracker {
  /**
   * Initialize the service
   *
   * @param conf Configuration
   * @param jobObserver Giraph job callbacks
   */
  void init(GiraphConfiguration conf, GiraphJobObserver jobObserver);

  /**
   * Set job
   *
   * @param job Job
   */
  void setJob(Job job);

  /**
   * Stop the thread which logs application progress and server
   *
   * @param succeeded Whether job succeeded or not
   */
  void stop(boolean succeeded);
}
