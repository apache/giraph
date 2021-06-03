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
package org.apache.giraph.utils;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Output committer which has abstract commit method
 */
public abstract class DefaultOutputCommitter extends OutputCommitter {
  /**
   * For cleaning up the job's output after job completion. Note that this
   * is invoked for jobs with final run state as
   * {@link org.apache.hadoop.mapreduce.JobStatus.State#SUCCEEDED}
   *
   * @param jobContext Context of the job whose output is being written.
   */
  public abstract void commit(JobContext jobContext) throws IOException;

  @Override
  public final void setupJob(JobContext jobContext) throws IOException {
  }

  @Override
  public final void setupTask(TaskAttemptContext taskContext)
      throws IOException {
  }

  @Override
  public final void commitJob(JobContext jobContext)
      throws IOException {
    super.commitJob(jobContext);
    commit(jobContext);
  }

  @Override
  public final boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    // Digraph does not require a task commit and there is a bug in Corona
    // which triggers t5688706
    // Avoiding the task commit should work around this.
    return false;
  }

  @Override
  public final void commitTask(TaskAttemptContext context) throws IOException {
  }

  @Override
  public final void abortTask(TaskAttemptContext taskContext)
      throws IOException {
  }
}
