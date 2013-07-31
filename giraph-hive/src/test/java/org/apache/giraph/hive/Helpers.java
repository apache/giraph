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
package org.apache.giraph.hive;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.internal.WrappedVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.HackJobContext;
import org.apache.hadoop.mapred.HackTaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class Helpers {
  public static InputStream getResource(String name) {
    return Helpers.class.getClassLoader().getResourceAsStream(name);
  }

  public static Map<Integer, Double> parseIntDoubleResults(Iterable<String> results) {
    Map<Integer, Double> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      double value = Double.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }

  public static Map<Integer, Integer> parseIntIntResults(Iterable<String> results) {
    Map<Integer, Integer> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      int value = Integer.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }

  public static void commitJob(GiraphConfiguration conf)
    throws IOException, InterruptedException {
    ImmutableClassesGiraphConfiguration iconf = new ImmutableClassesGiraphConfiguration(conf);
    WrappedVertexOutputFormat outputFormat = iconf.createWrappedVertexOutputFormat();
    JobConf jobConf = new JobConf(conf);
    TaskAttemptContext
        taskContext = new HackTaskAttemptContext(jobConf, new TaskAttemptID());
    OutputCommitter outputCommitter = outputFormat.getOutputCommitter(
        taskContext);
    JobContext jobContext = new HackJobContext(jobConf, taskContext.getJobID());
    outputCommitter.commitJob(jobContext);
  }

  public static JobContext makeJobContext(Configuration conf) {
    JobConf jobConf = new JobConf(conf);
    TaskAttemptContext
        taskContext = new HackTaskAttemptContext(jobConf, new TaskAttemptID());
    return new HackJobContext(jobConf, taskContext.getJobID());
  }
}
