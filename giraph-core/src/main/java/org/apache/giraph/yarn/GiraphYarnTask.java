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
package org.apache.giraph.yarn;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GraphTaskManager;

import org.apache.giraph.io.VertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This process will execute the BSP graph tasks alloted to this YARN
 * execution container. All tasks will be performed by calling the
 * GraphTaskManager object. Since this GiraphYarnTask will
 * not be passing data by key-value pairs through the MR framework, the
 * Mapper parameter types are irrelevant, and set to <code>Object</code> type.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class GiraphYarnTask<I extends WritableComparable, V extends Writable,
    E extends Writable> {
  static {
    Configuration.addDefaultResource(GiraphConstants.GIRAPH_YARN_CONF_FILE);
  }
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GiraphYarnTask.class);
  /** Manage the framework-agnostic Giraph task for this job run */
  private GraphTaskManager<I, V, E> graphTaskManager;
  /** Giraph task ID number must start @ index 0. Used by ZK, BSP, etc. */
  private final int bspTaskId;
  /** A special "dummy" override of Mapper#Context, used to deliver MRv1 deps */
  private Context proxy;
  /** Configuration to hand off into Giraph, through wrapper Mapper#Context */
  private ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor. Build our DUMMY MRv1 data structures to pass to our
   * GiraphTaskManager. This allows us to continue to look the other way
   * while Giraph relies on MRv1 under the hood.
   * @param taskAttemptId the MRv1 TaskAttemptID we constructed from CLI args
   *                      supplied by GiraphApplicationMaster.
   */
  public GiraphYarnTask(final TaskAttemptID taskAttemptId) {
    conf = new ImmutableClassesGiraphConfiguration<I, V, E>(
      new GiraphConfiguration());
    bspTaskId = taskAttemptId.getTaskID().getId();
    conf.setInt("mapred.task.partition", bspTaskId);
    proxy = buildProxyMapperContext(taskAttemptId);
    graphTaskManager = new GraphTaskManager<I, V, E>(proxy);
  }

  /**
   * Run one Giraph worker (or master) task, hosted in this execution container.
   */
  public void run() {
    // Notify the master quicker if there is worker failure rather than
    // waiting for ZooKeeper to timeout and delete the ephemeral znodes
    try {
      graphTaskManager.setup(null); // defaults GTM to "assume fatjar mode"
      graphTaskManager.execute();
      graphTaskManager.cleanup();
    } catch (InterruptedException ie) {
      LOG.error("run() caught an unrecoverable InterruptedException.", ie);
    } catch (IOException ioe) {
      throw new RuntimeException(
        "run() caught an unrecoverable IOException.", ioe);
      // CHECKSTYLE: stop IllegalCatch
    } catch (RuntimeException e) {
      // CHECKSTYLE: resume IllegalCatch
      graphTaskManager.zooKeeperCleanup();
      graphTaskManager.workerFailureCleanup();
      throw new RuntimeException(
        "run: Caught an unrecoverable exception " + e.getMessage(), e);
    } finally {
      // YARN: must complete the commit of the final output, Hadoop isn't there.
      finalizeYarnJob();
    }
  }

  /**
   * Without Hadoop MR to finish the consolidation of all the task output from
   * each HDFS task tmp dir, it won't get done. YARN has some job finalization
   * it must do "for us." -- AND must delete "jar cache" in HDFS too!
   */
  private void finalizeYarnJob() {
    if (conf.isPureYarnJob() && graphTaskManager.isMaster() &&
      conf.getVertexOutputFormatClass() != null) {
      try {
        LOG.info("Master is ready to commit final job output data.");
        VertexOutputFormat vertexOutputFormat =
          conf.createWrappedVertexOutputFormat();
        OutputCommitter outputCommitter =
          vertexOutputFormat.getOutputCommitter(proxy);
        // now we will have our output in OUTDIR if all went well...
        outputCommitter.commitJob(proxy);
        LOG.info("Master has committed the final job output data.");
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while attempting to obtain " +
          "OutputCommitter.", ie);
      } catch (IOException ioe) {
        LOG.error("Master task's attempt to commit output has " +
          "FAILED.", ioe);
      }
    }
  }

  /**
   * Utility to generate dummy Mapper#Context for use in Giraph internals.
   * This is the "key hack" to inject MapReduce-related data structures
   * containing YARN cluster metadata (and our GiraphConf from the AppMaster)
   * into our Giraph BSP task code.
   * @param tid the TaskAttemptID to construct this Mapper#Context from.
   * @return sort of a Mapper#Context if you squint just right.
   */
  private Context buildProxyMapperContext(final TaskAttemptID tid) {
    MapContext mc = new MapContextImpl<Object, Object, Object, Object>(
      conf, // our Configuration, populated back at the GiraphYarnClient.
      tid,  // our TaskAttemptId, generated w/YARN app, container, attempt IDs
      null, // RecordReader here will never be used by Giraph
      null, // RecordWriter here will never be used by Giraph
      null, // OutputCommitter here will never be used by Giraph
      new TaskAttemptContextImpl.DummyReporter() { // goes in task logs for now
        @Override
        public void setStatus(String msg) {
          LOG.info("[STATUS: task-" + bspTaskId + "] " + msg);
        }
      },
      null); // Input split setting here will never be used by Giraph

    // now, we wrap our MapContext ref so we can produce a Mapper#Context
    WrappedMapper<Object, Object, Object, Object> wrappedMapper
      = new WrappedMapper<Object, Object, Object, Object>();
    return wrappedMapper.getMapContext(mc);
  }

  /**
   * Task entry point.
   * @param args CLI arguments injected by GiraphApplicationMaster to hand off
   *             job, task, and attempt ID's to this (and every) Giraph task.
   *             Args should be: <code>AppId ContainerId AppAttemptId</code>
   */
  @SuppressWarnings("rawtypes")
  public static void main(String[] args) {
    if (args.length != 4) {
      throw new IllegalStateException("GiraphYarnTask could not construct " +
        "a TaskAttemptID for the Giraph job from args: " + printArgs(args));
    }
    try {
      GiraphYarnTask<?, ?, ?> giraphYarnTask =
        new GiraphYarnTask(getTaskAttemptID(args));
      giraphYarnTask.run();
      // CHECKSTYLE: stop IllegalCatch
    } catch (Throwable t) {
      // CHECKSTYLE resume IllegalCatch
      LOG.error("GiraphYarnTask threw a top-level exception, failing task", t);
      System.exit(2);
    } // ALWAYS finish a YARN task or AppMaster with System#exit!!!
    System.exit(0);
  }

  /**
   * Utility to create a TaskAttemptId we can feed to our fake Mapper#Context.
   *
   * NOTE: ContainerId will serve as MR TaskID for Giraph tasks.
   * YARN container 1 is always AppMaster, so the least container id we will
   * ever get from YARN for a Giraph task is container id 2. Giraph on MapReduce
   * tasks must start at index 0. So we SUBTRACT TWO from each container id.
   *
   * @param args the command line args, fed to us by GiraphApplicationMaster.
   * @return the TaskAttemptId object, populated with YARN job data.
   */
  private static TaskAttemptID getTaskAttemptID(String[] args) {
    return new TaskAttemptID(
      args[0], // YARN ApplicationId Cluster Timestamp
      Integer.parseInt(args[1]), // YARN ApplicationId #
      TaskID.getTaskType('m'),  // Make Giraph think this is a Mapper task.
      Integer.parseInt(args[2]) - 2, // YARN ContainerId MINUS TWO (see above)
      Integer.parseInt(args[3])); // YARN AppAttemptId #
  }

  /**
   * Utility to help log command line args in the event of an error.
   * @param args the CLI args.
   * @return a pretty-print of the input args.
   */
  private static String printArgs(String[] args) {
    int count = 0;
    StringBuilder sb = new StringBuilder();
    for (String arg : args) {
      sb.append("arg[" + (count++) + "] == " + arg + ", ");
    }
    return sb.toString();
  }
}
