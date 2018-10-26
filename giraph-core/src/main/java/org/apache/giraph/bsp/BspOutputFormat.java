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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This is for internal use only.  Allows the vertex output format routines
 * to be called as if a normal Hadoop job.
 */
public class BspOutputFormat extends OutputFormat<Text, Text> {
  /** Class logger */
  private static Logger LOG = Logger.getLogger(BspOutputFormat.class);

  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
    ImmutableClassesGiraphConfiguration conf =
        new ImmutableClassesGiraphConfiguration(context.getConfiguration());
    if (!conf.hasVertexOutputFormat() && !conf.hasEdgeOutputFormat()) {
      LOG.warn("checkOutputSpecs: ImmutableOutputCommiter" +
          " will not check anything");
      return;
    }

    if (conf.hasVertexOutputFormat()) {
      conf.createWrappedVertexOutputFormat().checkOutputSpecs(context);
    }
    if (conf.hasEdgeOutputFormat()) {
      conf.createWrappedEdgeOutputFormat().checkOutputSpecs(context);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    ImmutableClassesGiraphConfiguration conf =
        new ImmutableClassesGiraphConfiguration(context.getConfiguration());
    if (!conf.hasVertexOutputFormat() && !conf.hasEdgeOutputFormat()) {
      LOG.warn("getOutputCommitter: Returning " +
          "ImmutableOutputCommiter (does nothing).");
      return new ImmutableOutputCommitter();
    }

    if (conf.hasVertexOutputFormat()) {
      return conf.createWrappedVertexOutputFormat().getOutputCommitter(context);
    } else {
      return conf.createWrappedEdgeOutputFormat().getOutputCommitter(context);
    }
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new BspRecordWriter();
  }
}
