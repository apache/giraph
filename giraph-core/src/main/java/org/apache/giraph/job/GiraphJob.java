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

import com.google.common.collect.ImmutableList;
import org.apache.giraph.bsp.BspInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Generates an appropriate internal {@link Job} for using Giraph in Hadoop.
 * Uses composition to avoid unwanted {@link Job} methods from exposure
 * to the user.
 */
public class GiraphJob {
  static {
    Configuration.addDefaultResource("giraph-site.xml");
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphJob.class);
  /** Internal delegated job to proxy interface requests for Job */
  private final DelegatedJob delegatedJob;
  /** Name of the job */
  private String jobName;
  /** Helper configuration from the job */
  private final GiraphConfiguration giraphConfiguration;

  /**
   * Delegated job that simply passes along the class GiraphConfiguration.
   */
  private class DelegatedJob extends Job {
    /** Ensure that for job initiation the super.getConfiguration() is used */
    private boolean jobInited = false;

    /**
     * Constructor
     *
     * @param conf Configuration
     * @throws IOException
     */
    DelegatedJob(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public Configuration getConfiguration() {
      if (jobInited) {
        return giraphConfiguration;
      } else {
        return super.getConfiguration();
      }
    }
  }

  /**
   * Constructor that will instantiate the configuration
   *
   * @param jobName User-defined job name
   * @throws IOException
   */
  public GiraphJob(String jobName) throws IOException {
    this(new GiraphConfiguration(), jobName);
  }

  /**
   * Constructor.
   *
   * @param configuration User-defined configuration
   * @param jobName User-defined job name
   * @throws IOException
   */
  public GiraphJob(Configuration configuration,
                   String jobName) throws IOException {
    this(new GiraphConfiguration(configuration), jobName);
  }

  /**
   * Constructor.
   *
   * @param giraphConfiguration User-defined configuration
   * @param jobName User-defined job name
   * @throws IOException
   */
  public GiraphJob(GiraphConfiguration giraphConfiguration,
                   String jobName) throws IOException {
    this.jobName = jobName;
    this.giraphConfiguration = giraphConfiguration;
    this.delegatedJob = new DelegatedJob(giraphConfiguration);
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * Get the configuration from the internal job.
   *
   * @return Configuration used by the job.
   */
  public GiraphConfiguration getConfiguration() {
    return giraphConfiguration;
  }

  /**
   * Be very cautious when using this method as it returns the internal job
   * of {@link GiraphJob}.  This should only be used for methods that require
   * access to the actual {@link Job}, i.e. FileInputFormat#addInputPath().
   *
   * @return Internal job that will actually be submitted to Hadoop.
   */
  public Job getInternalJob() {
    delegatedJob.jobInited = true;
    return delegatedJob;
  }

  /**
   * Check if the configuration is local.  If it is local, do additional
   * checks due to the restrictions of LocalJobRunner. This checking is
   * performed here because the local job runner is MRv1-configured.
   *
   * @param conf Configuration
   */
  private static void checkLocalJobRunnerConfiguration(
      ImmutableClassesGiraphConfiguration conf) {
    String jobTracker = conf.get("mapred.job.tracker", null);
    if (!jobTracker.equals("local")) {
      // Nothing to check
      return;
    }

    int maxWorkers = conf.getMaxWorkers();
    if (maxWorkers != 1) {
      throw new IllegalArgumentException(
          "checkLocalJobRunnerConfiguration: When using " +
              "LocalJobRunner, must have only one worker since " +
          "only 1 task at a time!");
    }
    if (conf.getSplitMasterWorker()) {
      throw new IllegalArgumentException(
          "checkLocalJobRunnerConfiguration: When using " +
              "LocalJobRunner, you cannot run in split master / worker " +
          "mode since there is only 1 task at a time!");
    }
  }

  /**
   * Check whether a specified int conf value is set and if not, set it.
   *
   * @param param Conf value to check
   * @param defaultValue Assign to value if not set
   */
  private void setIntConfIfDefault(String param, int defaultValue) {
    if (giraphConfiguration.getInt(param, Integer.MIN_VALUE) ==
        Integer.MIN_VALUE) {
      giraphConfiguration.setInt(param, defaultValue);
    }
  }

  /**
   * Runs the actual graph application through Hadoop Map-Reduce.
   *
   * @param verbose If true, provide verbose output, false otherwise
   * @return True if success, false otherwise
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  public final boolean run(boolean verbose)
    throws IOException, InterruptedException, ClassNotFoundException {
    // Most users won't hit this hopefully and can set it higher if desired
    setIntConfIfDefault("mapreduce.job.counters.limit", 512);

    // Capacity scheduler-specific settings.  These should be enough for
    // a reasonable Giraph job
    setIntConfIfDefault("mapred.job.map.memory.mb", 1024);
    setIntConfIfDefault("mapred.job.reduce.memory.mb", 0);

    // Speculative execution doesn't make sense for Giraph
    giraphConfiguration.setBoolean(
        "mapred.map.tasks.speculative.execution", false);

    // Set the ping interval to 5 minutes instead of one minute
    // (DEFAULT_PING_INTERVAL)
    giraphConfiguration.setInt("ipc.ping.interval", 60000 * 5);

    // Should work in MAPREDUCE-1938 to let the user jars/classes
    // get loaded first
    giraphConfiguration.setBoolean("mapreduce.user.classpath.first", true);
    giraphConfiguration.setBoolean("mapreduce.job.user.classpath.first", true);

    // If the checkpoint frequency is 0 (no failure handling), set the max
    // tasks attempts to be 1 to encourage faster failure of unrecoverable jobs
    if (giraphConfiguration.getCheckpointFrequency() == 0) {
      int oldMaxTaskAttempts = giraphConfiguration.getMaxTaskAttempts();
      giraphConfiguration.setMaxTaskAttempts(1);
      if (LOG.isInfoEnabled()) {
        LOG.info("run: Since checkpointing is disabled (default), " +
            "do not allow any task retries (setting " +
            GiraphConstants.MAX_TASK_ATTEMPTS.getKey() + " = 1, " +
            "old value = " + oldMaxTaskAttempts + ")");
      }
    }

    // Set the job properties, check them, and submit the job
    ImmutableClassesGiraphConfiguration conf =
        new ImmutableClassesGiraphConfiguration(giraphConfiguration);
    checkLocalJobRunnerConfiguration(conf);

    int tryCount = 0;
    GiraphJobRetryChecker retryChecker = conf.getJobRetryChecker();
    while (true) {
      GiraphJobObserver jobObserver = conf.getJobObserver();

      JobProgressTrackerService jobProgressTrackerService =
          DefaultJobProgressTrackerService.createJobProgressTrackerService(
              conf, jobObserver);
      ClientThriftServer clientThriftServer = null;
      if (jobProgressTrackerService != null) {
        clientThriftServer = new ClientThriftServer(
            conf, ImmutableList.of(jobProgressTrackerService));
      }

      tryCount++;
      Job submittedJob = new Job(conf, jobName);
      if (submittedJob.getJar() == null) {
        submittedJob.setJarByClass(getClass());
      }
      submittedJob.setNumReduceTasks(0);
      submittedJob.setMapperClass(GraphMapper.class);
      submittedJob.setInputFormatClass(BspInputFormat.class);
      submittedJob.setOutputFormatClass(
          GiraphConstants.HADOOP_OUTPUT_FORMAT_CLASS.get(conf));
      if (jobProgressTrackerService != null) {
        jobProgressTrackerService.setJob(submittedJob);
      }

      jobObserver.launchingJob(submittedJob);
      submittedJob.submit();
      if (LOG.isInfoEnabled()) {
        LOG.info("Tracking URL: " + submittedJob.getTrackingURL());
        LOG.info(
            "Waiting for resources... Job will start only when it gets all " +
                (conf.getMinWorkers() + 1) + " mappers");
      }
      jobObserver.jobRunning(submittedJob);
      HaltApplicationUtils.printHaltInfo(submittedJob, conf);

      boolean passed = submittedJob.waitForCompletion(verbose);
      if (jobProgressTrackerService != null) {
        jobProgressTrackerService.stop(passed);
      }
      if (clientThriftServer != null) {
        clientThriftServer.stopThriftServer();
      }

      jobObserver.jobFinished(submittedJob, passed);

      if (!passed) {
        String restartFrom = retryChecker.shouldRestartCheckpoint(submittedJob);
        if (restartFrom != null) {
          GiraphConstants.RESTART_JOB_ID.set(conf, restartFrom);
          continue;
        }
      }

      if (passed || !retryChecker.shouldRetry(submittedJob, tryCount)) {
        return passed;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("run: Retrying job, " + tryCount + " try");
      }
    }
  }
}
