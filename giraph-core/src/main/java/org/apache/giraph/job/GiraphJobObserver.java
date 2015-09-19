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

import org.apache.hadoop.mapreduce.Job;

/**
 * An observer over the job launch lifecycle.
 */
public interface GiraphJobObserver {
  /**
   * Callback for job about to start.
   * @param jobToSubmit Job we're going to submit to hadoop.
   */
  void launchingJob(Job jobToSubmit);

  /**
   * Callback after job was submitted.
   * For example, you can track its progress here.
   * @param submittedJob Job which was submitted.
   */
  void jobRunning(Job submittedJob);

  /**
   * Callback when job finishes.
   * @param submittedJob Job that ran in hadoop.
   * @param passed true if job succeeded.
   */
  void jobFinished(Job submittedJob, boolean passed);

  /**
   * Called when job gets all mappers and
   * really starts computations.
   * May not get called if the job progress tracker
   * fails.
   * @param job job that runs
   */
  void jobGotAllMappers(Job job);
}
