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

package org.apache.giraph.bsp;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This output committer doesn't do anything, meant for the case
 * where output isn't desired, or as a base for not using
 * FileOutputCommitter.
 */
public class ImmutableOutputCommitter extends OutputCommitter {
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
    throws IOException {
    return false;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
  }

/*if[HADOOP_NON_SECURE]
  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {
  }

else[HADOOP_NON_SECURE]*/
/*end[HADOOP_NON_SECURE]*/
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
  }
}
