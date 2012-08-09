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
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.DefaultMasterCompute;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.io.PseudoRandomVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * Random Message Benchmark for evaluating the messaging performance.
 */
public class RandomMessageBenchmark implements Tool {
  /** How many supersteps to run */
  public static final String SUPERSTEP_COUNT =
      "RandomMessageBenchmark.superstepCount";
  /** How many bytes per message */
  public static final String NUM_BYTES_PER_MESSAGE =
      "RandomMessageBenchmark.numBytesPerMessage";
  /** Default bytes per message */
  public static final int DEFAULT_NUM_BYTES_PER_MESSAGE = 16;
  /** How many messages per edge */
  public static final String NUM_MESSAGES_PER_EDGE =
      "RandomMessageBenchmark.numMessagesPerEdge";
  /** Default messages per edge */
  public static final int DEFAULT_NUM_MESSAGES_PER_EDGE = 1;
  /** All bytes sent during this superstep */
  public static final String AGG_SUPERSTEP_TOTAL_BYTES =
      "superstep total bytes sent";
  /** All bytes sent during this application */
  public static final String AGG_TOTAL_BYTES = "total bytes sent";
  /** All messages during this superstep */
  public static final String AGG_SUPERSTEP_TOTAL_MESSAGES =
      "superstep total messages";
  /** All messages during this application */
  public static final String AGG_TOTAL_MESSAGES = "total messages";
  /** All millis during this superstep */
  public static final String AGG_SUPERSTEP_TOTAL_MILLIS =
      "superstep total millis";
  /** All millis during this application */
  public static final String AGG_TOTAL_MILLIS = "total millis";
  /** Workers for that superstep */
  public static final String WORKERS = "workers";
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(RandomMessageBenchmarkWorkerContext.class);
  /** Configuration from Configurable */
  private Configuration conf;

  /**
   * {@link WorkerContext} forRandomMessageBenchmark.
   */
  private static class RandomMessageBenchmarkWorkerContext extends
      WorkerContext {
    /** Class logger */
    private static final Logger LOG =
      Logger.getLogger(RandomMessageBenchmarkWorkerContext.class);
    /** Bytes to be sent */
    private byte[] messageBytes;
    /** Number of messages sent per edge */
    private int numMessagesPerEdge = -1;
    /** Number of supersteps */
    private int numSupersteps = -1;
    /** Random generator for random bytes message */
    private final Random random = new Random(System.currentTimeMillis());
    /** Start superstep millis */
    private long startSuperstepMillis = 0;
    /** Total bytes */
    private long totalBytes = 0;
    /** Total messages */
    private long totalMessages = 0;
    /** Total millis */
    private long totalMillis = 0;

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
      messageBytes =
        new byte[getContext().getConfiguration().
                 getInt(NUM_BYTES_PER_MESSAGE,
                 DEFAULT_NUM_BYTES_PER_MESSAGE)];
      numMessagesPerEdge =
          getContext().getConfiguration().
          getInt(NUM_MESSAGES_PER_EDGE,
              DEFAULT_NUM_MESSAGES_PER_EDGE);
      numSupersteps = getContext().getConfiguration().
          getInt(SUPERSTEP_COUNT, -1);
    }

    @Override
    public void preSuperstep() {
      long superstepBytes = this.<LongWritable>
          getAggregatedValue(AGG_SUPERSTEP_TOTAL_BYTES).get();
      long superstepMessages = this.<LongWritable>
          getAggregatedValue(AGG_SUPERSTEP_TOTAL_MESSAGES).get();
      long superstepMillis = this.<LongWritable>
          getAggregatedValue(AGG_SUPERSTEP_TOTAL_MILLIS).get();
      long workers = this.<LongWritable>getAggregatedValue(WORKERS).get();

      // For timing and tracking the supersteps
      // - superstep 0 starts the time, but cannot display any stats
      //   since nothing has been aggregated yet
      // - supersteps > 0 can display the stats
      if (getSuperstep() == 0) {
        startSuperstepMillis = System.currentTimeMillis();
      } else {
        totalBytes += superstepBytes;
        totalMessages += superstepMessages;
        totalMillis += superstepMillis;
        double superstepMegabytesPerSecond =
            superstepBytes * workers * 1000d / 1024d / 1024d / superstepMillis;
        double megabytesPerSecond = totalBytes *
            workers * 1000d / 1024d / 1024d / totalMillis;
        double superstepMessagesPerSecond =
            superstepMessages * workers * 1000d / superstepMillis;
        double messagesPerSecond =
            totalMessages * workers * 1000d / totalMillis;
        if (LOG.isInfoEnabled()) {
          LOG.info("Outputing statistics for superstep " + getSuperstep());
          LOG.info(AGG_SUPERSTEP_TOTAL_BYTES + " : " + superstepBytes);
          LOG.info(AGG_TOTAL_BYTES + " : " + totalBytes);
          LOG.info(AGG_SUPERSTEP_TOTAL_MESSAGES + " : " + superstepMessages);
          LOG.info(AGG_TOTAL_MESSAGES + " : " + totalMessages);
          LOG.info(AGG_SUPERSTEP_TOTAL_MILLIS + " : " + superstepMillis);
          LOG.info(AGG_TOTAL_MILLIS + " : " + totalMillis);
          LOG.info(WORKERS + " : " + workers);
          LOG.info("Superstep megabytes / second = " +
              superstepMegabytesPerSecond);
          LOG.info("Total megabytes / second = " +
              megabytesPerSecond);
          LOG.info("Superstep messages / second = " +
              superstepMessagesPerSecond);
          LOG.info("Total messages / second = " +
              messagesPerSecond);
          LOG.info("Superstep megabytes / second / worker = " +
              superstepMegabytesPerSecond / workers);
          LOG.info("Total megabytes / second / worker = " +
              megabytesPerSecond / workers);
          LOG.info("Superstep messages / second / worker = " +
              superstepMessagesPerSecond / workers);
          LOG.info("Total messages / second / worker = " +
              messagesPerSecond / workers);
        }
      }

      aggregate(WORKERS, new LongWritable(1));
    }

    @Override
    public void postSuperstep() {
      long endSuperstepMillis = System.currentTimeMillis();
      long superstepMillis = endSuperstepMillis - startSuperstepMillis;
      startSuperstepMillis = endSuperstepMillis;
      aggregate(AGG_SUPERSTEP_TOTAL_MILLIS, new LongWritable(superstepMillis));
    }

    @Override
    public void postApplication() { }

    /**
     * Get the message bytes to be used for sending.
     *
     * @return Byte array used for messages.
     */
    public byte[] getMessageBytes() {
      return messageBytes;
    }

    /**
     * Get the number of edges per message.
     *
     * @return Messages per edge.
     */
    public int getNumMessagePerEdge() {
      return numMessagesPerEdge;
    }

    /**
     * Get the number of supersteps.
     *
     * @return Number of supersteps.
     */
    public int getNumSupersteps() {
      return numSupersteps;
    }

    /**
     * Randomize the message bytes.
     */
    public void randomizeMessageBytes() {
      random.nextBytes(messageBytes);
    }
  }

  /**
   * Master compute associated with {@link RandomMessageBenchmark}.
   * It registers required aggregators.
   */
  public static class RandomMessageBenchmarkMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(AGG_SUPERSTEP_TOTAL_BYTES,
          LongSumAggregator.class);
      registerAggregator(AGG_SUPERSTEP_TOTAL_MESSAGES,
          LongSumAggregator.class);
      registerAggregator(AGG_SUPERSTEP_TOTAL_MILLIS,
          LongSumAggregator.class);
      registerAggregator(WORKERS,
          LongSumAggregator.class);
    }
  }

  /**
   * Actual message computation (messaging in this case)
   */
  public static class RandomMessageVertex extends EdgeListVertex<
      LongWritable, DoubleWritable, DoubleWritable, BytesWritable> {
    @Override
    public void compute(Iterable<BytesWritable> messages) {
      RandomMessageBenchmarkWorkerContext workerContext =
          (RandomMessageBenchmarkWorkerContext) getWorkerContext();
      if (getSuperstep() < workerContext.getNumSupersteps()) {
        for (int i = 0; i < workerContext.getNumMessagePerEdge(); i++) {
          workerContext.randomizeMessageBytes();
          sendMessageToAllEdges(
              new BytesWritable(workerContext.getMessageBytes()));
          long bytesSent = workerContext.getMessageBytes().length *
              getNumEdges();
          aggregate(AGG_SUPERSTEP_TOTAL_BYTES, new LongWritable(bytesSent));
          aggregate(AGG_SUPERSTEP_TOTAL_MESSAGES,
              new LongWritable(getNumEdges()));
        }
      } else {
        voteToHalt();
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Help");
    options.addOption("v", "verbose", false, "Verbose");
    options.addOption("w",
        "workers",
        true,
        "Number of workers");
    options.addOption("b",
        "bytes",
        true,
        "Message bytes per memssage");
    options.addOption("n",
        "number",
        true,
        "Number of messages per edge");
    options.addOption("s",
        "supersteps",
        true,
        "Supersteps to execute before finishing");
    options.addOption("V",
        "aggregateVertices",
        true,
        "Aggregate vertices");
    options.addOption("e",
        "edgesPerVertex",
        true,
        "Edges per vertex");
    options.addOption("f",
        "flusher",
        true,
        "Number of flush threads");

    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption('h')) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }
    if (!cmd.hasOption('w')) {
      LOG.info("Need to choose the number of workers (-w)");
      return -1;
    }
    if (!cmd.hasOption('s')) {
      LOG.info("Need to set the number of supersteps (-s)");
      return -1;
    }
    if (!cmd.hasOption('V')) {
      LOG.info("Need to set the aggregate vertices (-V)");
      return -1;
    }
    if (!cmd.hasOption('e')) {
      LOG.info("Need to set the number of edges " +
          "per vertex (-e)");
      return -1;
    }
    if (!cmd.hasOption('b')) {
      LOG.info("Need to set the number of message bytes (-b)");
      return -1;
    }
    if (!cmd.hasOption('n')) {
      LOG.info("Need to set the number of messages per edge (-n)");
      return -1;
    }
    int workers = Integer.parseInt(cmd.getOptionValue('w'));
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.getConfiguration().setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
    job.setVertexClass(RandomMessageVertex.class);
    job.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
    job.setWorkerContextClass(RandomMessageBenchmarkWorkerContext.class);
    job.setMasterComputeClass(RandomMessageBenchmarkMasterCompute.class);
    job.setWorkerConfiguration(workers, workers, 100.0f);
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.AGGREGATE_VERTICES,
        Long.parseLong(cmd.getOptionValue('V')));
    job.getConfiguration().setLong(
        PseudoRandomVertexInputFormat.EDGES_PER_VERTEX,
        Long.parseLong(cmd.getOptionValue('e')));
    job.getConfiguration().setInt(
        SUPERSTEP_COUNT,
        Integer.parseInt(cmd.getOptionValue('s')));
    job.getConfiguration().setInt(
        RandomMessageBenchmark.NUM_BYTES_PER_MESSAGE,
        Integer.parseInt(cmd.getOptionValue('b')));
    job.getConfiguration().setInt(
        RandomMessageBenchmark.NUM_MESSAGES_PER_EDGE,
        Integer.parseInt(cmd.getOptionValue('n')));

    boolean isVerbose = false;
    if (cmd.hasOption('v')) {
      isVerbose = true;
    }
    if (cmd.hasOption('s')) {
      getConf().setInt(SUPERSTEP_COUNT,
          Integer.parseInt(cmd.getOptionValue('s')));
    }
    if (cmd.hasOption('f')) {
      job.getConfiguration().setInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
          Integer.parseInt(cmd.getOptionValue('f')));
    }
    if (job.run(isVerbose)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically, this is the command line arguments.
   * @throws Exception Any exception thrown during computation.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new RandomMessageBenchmark(), args));
  }
}
