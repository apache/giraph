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

package org.apache.giraph.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.LogVersions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.util.Set;

/**
 * Abstract class which benchmarks should extend.
 */
public abstract class GiraphBenchmark implements Tool {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphBenchmark.class);
  /** Configuration */
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Set<BenchmarkOption> giraphOptions = getBenchmarkOptions();
    giraphOptions.add(BenchmarkOption.HELP);
    giraphOptions.add(BenchmarkOption.VERBOSE);
    giraphOptions.add(BenchmarkOption.WORKERS);
    Options options = new Options();
    for (BenchmarkOption giraphOption : giraphOptions) {
      giraphOption.addToOptions(options);
    }

    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    for (BenchmarkOption giraphOption : giraphOptions) {
      if (!giraphOption.checkOption(cmd, LOG)) {
        return -1;
      }
    }
    if (BenchmarkOption.HELP.optionTurnedOn(cmd)) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    int workers = Integer.parseInt(BenchmarkOption.WORKERS.getOptionValue(cmd));

    GiraphConfiguration giraphConf = job.getConfiguration();
    giraphConf.addWorkerObserverClass(LogVersions.class);
    giraphConf.addMasterObserverClass(LogVersions.class);

    giraphConf.setWorkerConfiguration(workers, workers, 100.0f);
    prepareConfiguration(giraphConf, cmd);

    boolean isVerbose = false;
    if (BenchmarkOption.VERBOSE.optionTurnedOn(cmd)) {
      isVerbose = true;
    }
    if (job.run(isVerbose)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Get the options to use in this benchmark.
   * BenchmarkOption.VERBOSE, BenchmarkOption.HELP and BenchmarkOption.WORKERS
   * will be added automatically, so you don't have to specify those.
   *
   * @return Options to use in this benchmark
   */
  public abstract Set<BenchmarkOption> getBenchmarkOptions();

  /**
   * Process options from CommandLine and prepare configuration for running
   * the job.
   * BenchmarkOption.VERBOSE, BenchmarkOption.HELP and BenchmarkOption.WORKERS
   * will be processed automatically so you don't have to process them.
   *
   * @param conf Configuration
   * @param cmd Command line
   */
  protected abstract void prepareConfiguration(GiraphConfiguration conf,
      CommandLine cmd);
}
