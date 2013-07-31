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
package org.apache.giraph.hive.jython;

import org.apache.giraph.graph.Language;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.jython.JythonJob;
import org.apache.giraph.scripting.DeployType;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.python.util.PythonInterpreter;

import com.facebook.hiveio.HiveIO;

import java.util.Arrays;

import static org.apache.giraph.hive.jython.HiveJythonUtils.parseJythonFiles;
import static org.apache.giraph.utils.DistributedCacheUtils.copyAndAdd;

/**
 * Runner for jobs written in Jython
 */
public class HiveJythonRunner implements Tool {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(HiveJythonRunner.class);
  /** Configuration */
  private static HiveConf CONF = new HiveConf();

  @Override public int run(String[] args) throws Exception {
    args = HiveJythonUtils.processArgs(args, CONF);
    LOG.info("Processed hive options now have args: " + Arrays.toString(args));

    HiveIO.init(CONF, false);

    PythonInterpreter interpreter = new PythonInterpreter();

    JythonJob jythonJob = parseJythonFiles(interpreter, args);

    logOptions();

    for (String arg : args) {
      Path remoteScriptPath = copyAndAdd(new Path(arg), CONF);
      ScriptLoader.addScriptToLoad(CONF, remoteScriptPath.toString(),
          DeployType.DISTRIBUTED_CACHE, Language.JYTHON);
    }

    String name = HiveJythonUtils.writeJythonJobToConf(jythonJob, CONF,
       interpreter);

    GiraphJob job = new GiraphJob(CONF, name);
    return job.run(true) ? 0 : -1;
  }

  /**
   * Log options used
   */
  private static void logOptions() {
    StringBuilder sb = new StringBuilder(100);
    appendEnvVars(sb, "JAVA_HOME", "MAPRED_POOL_NAME");
    appendEnvVars(sb, "HADOOP_HOME", "HIVE_HOME");
    LOG.info("Environment:\n" + sb);
  }

  /**
   * Append environment variables to StringBuilder
   *
   * @param sb StringBuilder
   * @param names vararg of env keys
   */
  private static void appendEnvVars(StringBuilder sb, String ... names) {
    for (String name : names) {
      sb.append(name).append("=").append(System.getenv(name)).append("\n");
    }
  }

  /**
   * Set the static configuration stored
   *
   * @param conf Configuration
   */
  public static void setStaticConf(Configuration conf) {
    if (conf instanceof HiveConf) {
      HiveJythonRunner.CONF = (HiveConf) conf;
    } else {
      HiveJythonRunner.CONF = new HiveConf(conf, HiveJythonRunner.class);
    }
  }

  @Override public void setConf(Configuration conf) {
    setStaticConf(conf);
  }

  @Override public Configuration getConf() {
    return CONF;
  }

  /**
   * Entry point
   *
   * @param args command line args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new HiveJythonRunner(), args));
  }
}
