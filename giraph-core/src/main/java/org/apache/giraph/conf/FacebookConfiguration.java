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

package org.apache.giraph.conf;

import org.apache.commons.lang3.StringUtils;
import org.apache.giraph.comm.flow_control.StaticFlowControl;
import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.master.BspServiceMaster;
import org.apache.giraph.worker.MemoryObserver;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Default configuration used in Facebook
 */
public class FacebookConfiguration implements BulkConfigurator {
  /**
   * How much memory per mapper should we use
   */
  public static final IntConfOption MAPPER_MEMORY =
      new IntConfOption("giraph.mapperMemoryGb", 10,
          "How many GBs of memory to give to the mappers");
  /**
   * How many cores per mapper should we use
   */
  public static final IntConfOption MAPPER_CORES =
      new IntConfOption("giraph.mapperCores", 10,
          "How many cores will mapper be allowed to use");

  /**
   * Fraction of {@link #MAPPER_MEMORY} to use for new generation
   */
  public static final FloatConfOption NEW_GEN_MEMORY_FRACTION =
      new FloatConfOption("giraph.newGenMemoryFraction", 0.1f,
          "Fraction of total mapper memory to use for new generation");
  /**
   * Note: using G1 is often faster, but we've seen it add off heap memory
   * overhead which can cause issues.
   */
  public static final BooleanConfOption USE_G1_COLLECTOR =
      new BooleanConfOption("giraph.useG1Collector", false,
          "Whether or not to use G1 garbage collector");
  /**
   * Which fraction of cores to use for threads when computation and
   * communication overlap
   */
  public static final FloatConfOption CORES_FRACTION_DURING_COMMUNICATION =
      new FloatConfOption("giraph.coresFractionDuringCommunication", 0.7f,
          "Fraction of mapper cores to use for threads which overlap with" +
              " network communication");

  /**
   * Whether to configure java opts.
   */
  public static final BooleanConfOption CONFIGURE_JAVA_OPTS =
      new BooleanConfOption("giraph.configureJavaOpts", true,
          "Whether to configure java opts");

  /**
   * Java options passed to mappers.
   */
  public static final StrConfOption MAPRED_JAVA_JOB_OPTIONS =
      new StrConfOption("mapred.child.java.opts", null,
          "Java options passed to mappers");

  /**
   * Expand GiraphConfiguration with default Facebook settings.
   * Assumes {@link #MAPPER_CORES} and number of workers to use
   * are already set correctly in Configuration.
   *
   * For all conf options it changed it will only do so if they are not set,
   * so it won't override any of your custom settings. The only exception is
   * mapred.child.java.opts, this one will be overwritten depending on the
   * {@link #CONFIGURE_JAVA_OPTS} setting
   *
   * @param conf Configuration
   */
  @Override
  public void configure(GiraphConfiguration conf) {
    int workers = conf.getInt(GiraphConstants.MIN_WORKERS, -1);
    Preconditions.checkArgument(workers > 0, "Number of workers not set");
    int cores = MAPPER_CORES.get(conf);

    // Nothing else happens while we write input splits to zk,
    // so we can use all threads
    conf.setIfUnset(BspServiceMaster.NUM_MASTER_ZK_INPUT_SPLIT_THREADS,
        Integer.toString(cores));
    // Nothing else happens while we write output, so we can use all threads
    GiraphConstants.NUM_OUTPUT_THREADS.setIfUnset(conf, cores);

    int threadsDuringCommunication = Math.max(1,
        (int) (cores * CORES_FRACTION_DURING_COMMUNICATION.get(conf)));
    // Input overlaps with communication, set threads properly
    GiraphConstants.NUM_INPUT_THREADS.setIfUnset(
        conf, threadsDuringCommunication);
    // Compute overlaps with communication, set threads properly
    GiraphConstants.NUM_COMPUTE_THREADS.setIfUnset(
        conf, threadsDuringCommunication);
    // Netty server threads are the ones adding messages to stores,
    // or adding vertices and edges to stores during input,
    // these are expensive operations so set threads properly
    GiraphConstants.NETTY_SERVER_THREADS.setIfUnset(
        conf, threadsDuringCommunication);

    // Ensure we can utilize all communication threads by having enough
    // channels per server, in cases when we have just a few machines
    GiraphConstants.CHANNELS_PER_SERVER.setIfUnset(conf,
        Math.max(1, 2 * threadsDuringCommunication / workers));

    // Limit number of open requests to 2000
    NettyClient.LIMIT_NUMBER_OF_OPEN_REQUESTS.setIfUnset(conf, true);
    StaticFlowControl.MAX_NUMBER_OF_OPEN_REQUESTS.setIfUnset(conf, 100);
    // Pooled allocator in netty is faster
    GiraphConstants.NETTY_USE_POOLED_ALLOCATOR.setIfUnset(conf, true);
    // Turning off auto read is faster
    GiraphConstants.NETTY_AUTO_READ.setIfUnset(conf, false);

    // Synchronize full gc calls across workers
    MemoryObserver.USE_MEMORY_OBSERVER.setIfUnset(conf, true);

    // Increase number of partitions per compute thread
    GiraphConstants.MIN_PARTITIONS_PER_COMPUTE_THREAD.setIfUnset(conf, 3);

    // Prefer ip addresses
    GiraphConstants.PREFER_IP_ADDRESSES.setIfUnset(conf, true);

    // Track job progress
    GiraphConstants.TRACK_JOB_PROGRESS_ON_CLIENT.setIfUnset(conf, true);
    // Thread-level debugging for easier understanding
    GiraphConstants.LOG_THREAD_LAYOUT.setIfUnset(conf, true);
    // Enable tracking and printing of metrics
    GiraphConstants.METRICS_ENABLE.setIfUnset(conf, true);

    if (CONFIGURE_JAVA_OPTS.get(conf)) {
      List<String> javaOpts = getMemoryJavaOpts(conf);
      javaOpts.addAll(getGcJavaOpts(conf));
      MAPRED_JAVA_JOB_OPTIONS.set(conf, StringUtils.join(javaOpts, " "));
    }
  }

  /**
   * Get memory java opts to use
   *
   * @param conf Configuration
   * @return Java opts
   */
  public static List<String> getMemoryJavaOpts(Configuration conf) {
    int memoryGb = MAPPER_MEMORY.get(conf);
    List<String> javaOpts = new ArrayList<>();
    // Set xmx and xms to the same value
    javaOpts.add("-Xms" + memoryGb + "g");
    javaOpts.add("-Xmx" + memoryGb + "g");
    // Non-uniform memory allocator (great for multi-threading and appears to
    // have no impact when single threaded)
    javaOpts.add("-XX:+UseNUMA");
    return javaOpts;
  }

  /**
   * Get garbage collection java opts to use
   *
   * @param conf Configuration
   * @return Java opts
   */
  public static List<String> getGcJavaOpts(Configuration conf) {
    List<String> gcJavaOpts = new ArrayList<>();
    if (USE_G1_COLLECTOR.get(conf)) {
      gcJavaOpts.add("-XX:+UseG1GC");
      gcJavaOpts.add("-XX:MaxGCPauseMillis=500");
    } else {
      int newGenMemoryGb = Math.max(1,
          (int) (MAPPER_MEMORY.get(conf) * NEW_GEN_MEMORY_FRACTION.get(conf)));
      // Use parallel gc collector
      gcJavaOpts.add("-XX:+UseParallelGC");
      gcJavaOpts.add("-XX:+UseParallelOldGC");
      // Fix new size generation
      gcJavaOpts.add("-XX:NewSize=" + newGenMemoryGb + "g");
      gcJavaOpts.add("-XX:MaxNewSize=" + newGenMemoryGb + "g");
    }
    return gcJavaOpts;
  }
}
