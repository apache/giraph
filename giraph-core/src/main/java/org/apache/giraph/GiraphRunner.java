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
package org.apache.giraph;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.io.formats.FileOutputFormatUtil;
import org.apache.giraph.utils.ConfigurationUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
/*if[PURE_YARN]
import org.apache.giraph.yarn.GiraphYarnClient;
end[PURE_YARN]*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.net.URI;

/**
 * Helper class to run Giraph applications by specifying the actual class name
 * to use (i.e. vertex, vertex input/output format, combiner, etc.).
 *
 * This is the default entry point for Giraph jobs running on any Hadoop
 * cluster, MRv1 or v2, including Hadoop-specific configuration and setup.
 */
public class GiraphRunner implements Tool {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphRunner.class);
  /** Writable conf */
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  /**
   * Drives a job run configured for "Giraph on Hadoop MR cluster"
   * @param args the command line arguments
   * @return job run exit code
   */
  public int run(String[] args) throws Exception {
    if (null == getConf()) { // for YARN profile
      conf = new Configuration();
    }
    GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
    CommandLine cmd = ConfigurationUtils.parseArgs(giraphConf, args);
    if (null == cmd) {
      return 0; // user requested help/info printout, don't run a job.
    }

    // set up job for various platforms
    final String vertexClassName = args[0];
    final String jobName = "Giraph: " + vertexClassName;
/*if[PURE_YARN]
    GiraphYarnClient job = new GiraphYarnClient(giraphConf, jobName);
else[PURE_YARN]*/
    GiraphJob job = new GiraphJob(giraphConf, jobName);
    prepareHadoopMRJob(job, cmd);
/*end[PURE_YARN]*/

    // run the job, collect results
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to run Vertex: " + vertexClassName);
    }
    boolean verbose = !cmd.hasOption('q');
    return job.run(verbose) ? 0 : -1;
  }

  /**
   * Populate internal Hadoop Job (and Giraph IO Formats) with Hadoop-specific
   * configuration/setup metadata, propagating exceptions to calling code.
   * @param job the GiraphJob object to help populate Giraph IO Format data.
   * @param cmd the CommandLine for parsing Hadoop MR-specific args.
   */
  private void prepareHadoopMRJob(final GiraphJob job, final CommandLine cmd)
    throws Exception {
    if (cmd.hasOption("vof") || cmd.hasOption("eof")) {
      if (cmd.hasOption("op")) {
        FileOutputFormatUtil.setOutputPath(job.getInternalJob(),
          new Path(cmd.getOptionValue("op")));
      }
    }
    if (cmd.hasOption("cf")) {
      DistributedCache.addCacheFile(new URI(cmd.getOptionValue("cf")),
          job.getConfiguration());
    }
  }

  /**
   * Execute GiraphRunner.
   *
   * @param args Typically command line arguments.
   * @throws Exception Any exceptions thrown.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new GiraphRunner(), args));
  }
}
