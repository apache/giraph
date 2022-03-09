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

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.SimpleSuperstepComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that worker context and master computation
 * are properly saved and loaded back at checkpoint.
 */
public class TestCheckpointing extends BspCase {

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TestCheckpointing.class);
  /** ID to be used with test job */
  public static final String TEST_JOB_ID = "test_job";

  private static SuperstepCallback SUPERSTEP_CALLBACK;

  /**
   * Create the test case
   */
  public TestCheckpointing() {
    super(TestCheckpointing.class.getName());
  }

  @Test
  public void testBspCheckpoint() throws InterruptedException, IOException, ClassNotFoundException {
    testBspCheckpoint(false);
  }

  @Test
  public void testAsyncMessageStoreCheckpoint() throws InterruptedException, IOException, ClassNotFoundException {
    testBspCheckpoint(true);
  }

  public void testBspCheckpoint(boolean useAsyncMessageStore)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path checkpointsDir = getTempPath("checkpointing");
    GiraphConfiguration conf = new GiraphConfiguration();
    if (useAsyncMessageStore) {
      GiraphConstants.ASYNC_MESSAGE_STORE_THREADS_COUNT.set(conf, 2);
    }

    SUPERSTEP_CALLBACK = null;

    GiraphConstants.CLEANUP_CHECKPOINTS_AFTER_SUCCESS.set(conf, false);
    conf.setCheckpointFrequency(2);

    long idSum = runOriginalJob(checkpointsDir, conf);
    assertEquals(10, idSum);

    SUPERSTEP_CALLBACK = new SuperstepCallback() {
      @Override
      public void superstep(long superstep,
                            ImmutableClassesGiraphConfiguration<LongWritable, IntWritable, FloatWritable> conf) {
        if (superstep < 2) {
          Assert.fail("Restarted JOB should not be executed on superstep " + superstep);
        }
      }
    };

    runRestartedJob(checkpointsDir, conf, idSum, 2);


  }

  private void runRestartedJob(Path checkpointsDir, GiraphConfiguration conf, long idSum, long restartFrom) throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath;
    LOG.info("testBspCheckpoint: Restarting from the latest superstep " +
        "with checkpoint path = " + checkpointsDir);
    outputPath = getTempPath("checkpointing_restarted");

    GiraphConstants.RESTART_JOB_ID.set(conf, TEST_JOB_ID);
    conf.set("mapred.job.id", "restarted_test_job");
    if (restartFrom >= 0) {
      conf.set(GiraphConstants.RESTART_SUPERSTEP, Long.toString(restartFrom));
    }

    GiraphJob restartedJob = prepareJob(getCallingMethodName() + "Restarted",
        conf, outputPath);

    GiraphConstants.CHECKPOINT_DIRECTORY.set(restartedJob.getConfiguration(),
        checkpointsDir.toString());

    assertTrue(restartedJob.run(true));


    if (!runningInDistributedMode()) {
      long idSumRestarted =
          CheckpointVertexWorkerContext
              .getFinalSum();
      LOG.info("testBspCheckpoint: idSumRestarted = " +
          idSumRestarted);
      assertEquals(idSum, idSumRestarted);
    }
  }

  private long runOriginalJob(Path checkpointsDir,  GiraphConfiguration conf) throws IOException, InterruptedException, ClassNotFoundException {
    Path outputPath = getTempPath("checkpointing_original");
    conf.setComputationClass(
        CheckpointComputation.class);
    conf.setWorkerContextClass(
        CheckpointVertexWorkerContext.class);
    conf.setMasterComputeClass(
        CheckpointVertexMasterCompute.class);
    conf.setVertexInputFormatClass(SimpleSuperstepComputation.SimpleSuperstepVertexInputFormat.class);
    conf.setVertexOutputFormatClass(SimpleSuperstepComputation.SimpleSuperstepVertexOutputFormat.class);
    conf.set("mapred.job.id", TEST_JOB_ID);
    GiraphJob job = prepareJob(getCallingMethodName(), conf, outputPath);

    GiraphConfiguration configuration = job.getConfiguration();
    GiraphConstants.CHECKPOINT_DIRECTORY.set(configuration, checkpointsDir.toString());

    assertTrue(job.run(true));

    long idSum = 0;
    if (!runningInDistributedMode()) {
      FileStatus fileStatus = getSinglePartFileStatus(job.getConfiguration(),
          outputPath);
      idSum = CheckpointVertexWorkerContext
          .getFinalSum();
      LOG.info("testBspCheckpoint: idSum = " + idSum +
          " fileLen = " + fileStatus.getLen());
    }
    return idSum;
  }


  /**
   * Actual computation.
   */
  public static class CheckpointComputation extends
      BasicComputation<LongWritable, IntWritable, FloatWritable,
          FloatWritable> {
    @Override
    public void compute(
        Vertex<LongWritable, IntWritable, FloatWritable> vertex,
        Iterable<FloatWritable> messages) throws IOException {
      CheckpointVertexWorkerContext workerContext = getWorkerContext();
      assertEquals(getSuperstep() + 1, workerContext.testValue);

      if (getSuperstep() > 4) {
        vertex.voteToHalt();
        return;
      }

      aggregate(LongSumAggregator.class.getName(),
          new LongWritable(vertex.getId().get()));

      float msgValue = 0.0f;
      for (FloatWritable message : messages) {
        float curMsgValue = message.get();
        msgValue += curMsgValue;
      }

      int vertexValue = vertex.getValue().get();
      vertex.setValue(new IntWritable(vertexValue + (int) msgValue));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        FloatWritable newEdgeValue = new FloatWritable(edge.getValue().get() +
            (float) vertexValue);
        Edge<LongWritable, FloatWritable> newEdge =
            EdgeFactory.create(edge.getTargetVertexId(), newEdgeValue);
        vertex.addEdge(newEdge);
        sendMessage(edge.getTargetVertexId(), newEdgeValue);

      }
    }
  }

  @Test
  public void testManualCheckpointAtTheBeginning()
      throws InterruptedException, IOException, ClassNotFoundException {
    testManualCheckpoint(0);
  }

  @Test
  public void testManualCheckpoint()
      throws InterruptedException, IOException, ClassNotFoundException {
    testManualCheckpoint(2);
  }


  private void testManualCheckpoint(final int checkpointSuperstep)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path checkpointsDir = getTempPath("checkpointing");
    GiraphConfiguration conf = new GiraphConfiguration();

    SUPERSTEP_CALLBACK = new SuperstepCallback() {

      @Override
      public void superstep(long superstep, ImmutableClassesGiraphConfiguration<LongWritable, IntWritable, FloatWritable> conf) {
        if (superstep == checkpointSuperstep) {
          try {
            ZooKeeperExt zooKeeperExt = new ZooKeeperExt(conf.getZookeeperList(),
                conf.getZooKeeperSessionTimeout(),
                conf.getZookeeperOpsMaxAttempts(),
                conf.getZookeeperOpsRetryWaitMsecs(),
                TestCheckpointing.this);
            String basePath = ZooKeeperManager.getBasePath(conf) + BspService.BASE_DIR + "/" + conf.get("mapred.job.id");
            zooKeeperExt.createExt(
                basePath + BspService.FORCE_CHECKPOINT_USER_FLAG,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                true);
          } catch (IOException | InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
          }
        } else if (superstep > checkpointSuperstep) {
          Assert.fail("Job should be stopped by now " + superstep);
        }
      }
    };

    try {
      runOriginalJob(checkpointsDir, conf);
      fail("Original job should fail after checkpointing");
    } catch (Exception e) {
      LOG.info("Original job failed, that's OK " + e);
    }

    SUPERSTEP_CALLBACK = new SuperstepCallback() {
      @Override
      public void superstep(long superstep,
                            ImmutableClassesGiraphConfiguration<LongWritable, IntWritable, FloatWritable> conf) {
        if (superstep < checkpointSuperstep) {
          Assert.fail("Restarted JOB should not be executed on superstep " + superstep);
        }
      }
    };

    runRestartedJob(checkpointsDir, conf, 10, -1);
  }

  /**
   * Worker context associated.
   */
  public static class CheckpointVertexWorkerContext
      extends DefaultWorkerContext {
    /** User can access this after the application finishes if local */
    private static long FINAL_SUM;

    private int testValue;

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void postSuperstep() {
      super.postSuperstep();
      sendMessageToMyself(new LongWritable(getSuperstep()));
    }

    /**
     * Send message to all workers (except this worker)
     *
     * @param message Message to send
     */
    private void sendMessageToMyself(Writable message) {
      sendMessageToWorker(message, getMyWorkerIndex());
    }

    @Override
    public void postApplication() {
      setFinalSum(this.<LongWritable>getAggregatedValue(
          LongSumAggregator.class.getName()).get());
      LOG.info("FINAL_SUM=" + FINAL_SUM);
    }

    /**
     * Set the final sum
     *
     * @param value sum
     */
    private static void setFinalSum(long value) {
      FINAL_SUM = value;
    }

    @Override
    public void preSuperstep() {
      assertEquals(getSuperstep(), testValue++);
      if (getSuperstep() > 0) {
        List<Writable> messages = getAndClearMessagesFromOtherWorkers();
        assertEquals(1, messages.size());
        assertEquals(getSuperstep() - 1, ((LongWritable)(messages.get(0))).get());
      }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      super.readFields(dataInput);
      testValue = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      super.write(dataOutput);
      dataOutput.writeInt(testValue);
    }
  }

  /**
   * Master compute
   */
  public static class CheckpointVertexMasterCompute extends
      DefaultMasterCompute {

    private int testValue = 0;

    @Override
    public void compute() {
      long superstep = getSuperstep();
      if (SUPERSTEP_CALLBACK != null) {
        SUPERSTEP_CALLBACK.superstep(getSuperstep(), getConf());
      }
      assertEquals(superstep, testValue++);
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(LongSumAggregator.class.getName(),
          LongSumAggregator.class);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      testValue = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      out.writeInt(testValue);
    }
  }

  private static interface SuperstepCallback {

    public void superstep(long superstep,
                          ImmutableClassesGiraphConfiguration<LongWritable,
                              IntWritable, FloatWritable> conf);

  }

}
