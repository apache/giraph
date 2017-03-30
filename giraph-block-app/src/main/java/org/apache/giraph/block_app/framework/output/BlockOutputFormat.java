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
package org.apache.giraph.block_app.framework.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.bsp.BspOutputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.utils.ConfigurationObjectUtils;
import org.apache.giraph.utils.DefaultOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Hadoop output format to use with block output.
 * It keeps track of all registered outputs, and knows how to create them.
 */
public class BlockOutputFormat extends BspOutputFormat {
  private static final StrConfOption OUTPUT_CONF_OPTIONS = new StrConfOption(
      "giraph.outputConfOptions", "",
      "List of conf options for outputs used");

  public static <OD> void addOutputDesc(OD outputDesc, String confOption,
      GiraphConfiguration conf) {
    GiraphConstants.HADOOP_OUTPUT_FORMAT_CLASS.set(conf,
        BlockOutputFormat.class);
    String currentOutputs = OUTPUT_CONF_OPTIONS.get(conf);
    if (!currentOutputs.isEmpty()) {
      currentOutputs = currentOutputs + ",";
    }
    OUTPUT_CONF_OPTIONS.set(conf, currentOutputs + confOption);
    ConfigurationObjectUtils.setObjectKryo(outputDesc, confOption, conf);
  }

  /**
   * Returns an array of output configuration options set in the input
   * configuration.
   *
   * @param conf Configuration
   * @return Array of options
   */
  public static String[] getOutputConfOptions(Configuration conf) {
    String outputConfOptions = OUTPUT_CONF_OPTIONS.get(conf);
    return outputConfOptions.isEmpty() ?
        new String[0] : outputConfOptions.split(",");
  }

  public static <OW extends BlockOutputWriter, OD extends BlockOutputDesc<OW>>
  OD createInitAndCheckOutputDesc(String confOption, Configuration conf,
      String jobIdentifier) {
    OD outputDesc = ConfigurationObjectUtils.getObjectKryo(confOption, conf);
    outputDesc.initializeAndCheck(jobIdentifier, conf);
    return outputDesc;
  }

  public static Map<String, BlockOutputDesc>
  createInitAndCheckOutputDescsMap(Configuration conf, String jobIdentifier) {
    String[] outputConfOptions = getOutputConfOptions(conf);
    Map<String, BlockOutputDesc> ret = new HashMap<>(outputConfOptions.length);
    for (String outputConfOption : outputConfOptions) {
      ret.put(outputConfOption,
          createInitAndCheckOutputDesc(outputConfOption, conf, jobIdentifier));
    }
    return ret;
  }

  public static Map<String, BlockOutputDesc> createInitAndCheckOutputDescsMap(
      JobContext jobContext) {
    return createInitAndCheckOutputDescsMap(jobContext.getConfiguration(),
        jobContext.getJobID().toString());
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext)
      throws IOException, InterruptedException {
    createInitAndCheckOutputDescsMap(jobContext);
  }

  @Override
  public OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new DefaultOutputCommitter() {
      @Override
      public void commit(JobContext jobContext) throws IOException {
        Map<String, BlockOutputDesc> map =
            createInitAndCheckOutputDescsMap(jobContext);
        for (BlockOutputDesc outputDesc : map.values()) {
          outputDesc.commit();
        }
      }
    };
  }
}
