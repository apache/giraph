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

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Default implementation of job observer that does nothing.
 */
public class DefaultJobObserver implements GiraphJobObserver,
    ImmutableClassesGiraphConfigurable {
  /** configuration object stored here */
  private ImmutableClassesGiraphConfiguration conf;

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.conf = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return this.conf;
  }

  @Override
  public void launchingJob(Job jobToSubmit) {
    // do nothing
  }

  @Override
  public void jobRunning(Job submittedJob) {
    // do nothing
  }

  @Override
  public void jobFinished(Job jobToSubmit, boolean passed) {
    // do nothing
  }

  @Override
  public void jobGotAllMappers(Job job) {
    // do nothing
  }
}
