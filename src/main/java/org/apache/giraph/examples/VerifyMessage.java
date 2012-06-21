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

package org.apache.giraph.examples;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * An example that simply uses its id, value, and edges to compute new data
 * every iteration to verify that messages are sent and received at the
 * appropriate location and superstep.
 */
public class VerifyMessage {
  /**
   * Message that will be sent in {@link VerifyMessageVertex}.
   */
  public static class VerifiableMessage implements Writable {
    /** Superstep sent on */
    private long superstep;
    /** Source vertex id */
    private long sourceVertexId;
    /** Value */
    private float value;

    /**
     * Default constructor used with reflection.
     */
    public VerifiableMessage() { }

    /**
     * Constructor with verifiable arguments.
     * @param superstep Superstep this message was created on.
     * @param sourceVertexId Who send this message.
     * @param value A value associated with this message.
     */
    public VerifiableMessage(
        long superstep, long sourceVertexId, float value) {
      this.superstep = superstep;
      this.sourceVertexId = sourceVertexId;
      this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      superstep = input.readLong();
      sourceVertexId = input.readLong();
      value = input.readFloat();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeLong(superstep);
      output.writeLong(sourceVertexId);
      output.writeFloat(value);
    }

    @Override
    public String toString() {
      return "(superstep=" + superstep + ",sourceVertexId=" +
          sourceVertexId + ",value=" + value + ")";
    }
  }

  /**
   * Send and verify messages.
   */
  public static class VerifyMessageVertex extends
      EdgeListVertex<LongWritable, IntWritable, FloatWritable,
      VerifiableMessage> {
    /** Dynamically set number of SUPERSTEPS */
    public static final String SUPERSTEP_COUNT =
        "verifyMessageVertex.superstepCount";
    /** User can access this after the application finishes if local */
    private static long FINAL_SUM;
    /** Number of SUPERSTEPS to run (6 by default) */
    private static int SUPERSTEPS = 6;
    /** Class logger */
    private static Logger LOG = Logger.getLogger(VerifyMessageVertex.class);

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    /**
     * Worker context used with {@link VerifyMessageVertex}.
     */
    public static class VerifyMessageVertexWorkerContext extends
        WorkerContext {
      @Override
      public void preApplication() throws InstantiationException,
      IllegalAccessException {
        registerAggregator(LongSumAggregator.class.getName(),
            LongSumAggregator.class);
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        sumAggregator.setAggregatedValue(0);
        SUPERSTEPS = getContext().getConfiguration().getInt(
            SUPERSTEP_COUNT, SUPERSTEPS);
      }

      @Override
      public void postApplication() {
        LongSumAggregator sumAggregator = (LongSumAggregator)
            getAggregator(LongSumAggregator.class.getName());
        FINAL_SUM = sumAggregator.getAggregatedValue().get();
      }

      @Override
      public void preSuperstep() {
        useAggregator(LongSumAggregator.class.getName());
      }

      @Override
      public void postSuperstep() { }
    }

    @Override
    public void compute(Iterator<VerifiableMessage> msgIterator) {
      LongSumAggregator sumAggregator = (LongSumAggregator)
          getAggregator(LongSumAggregator.class.getName());
      if (getSuperstep() > SUPERSTEPS) {
        voteToHalt();
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: " + sumAggregator);
      }
      sumAggregator.aggregate(getVertexId().get());
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: sum = " +
            sumAggregator.getAggregatedValue().get() +
            " for vertex " + getVertexId());
      }
      float msgValue = 0.0f;
      while (msgIterator.hasNext()) {
        VerifiableMessage msg = msgIterator.next();
        msgValue += msg.value;
        if (LOG.isDebugEnabled()) {
          LOG.debug("compute: got msg = " + msg +
              " for vertex id " + getVertexId() +
              ", vertex value " + getVertexValue() +
              " on superstep " + getSuperstep());
        }
        if (msg.superstep != getSuperstep() - 1) {
          throw new IllegalStateException(
              "compute: Impossible to not get a messsage from " +
                  "the previous superstep, current superstep = " +
                  getSuperstep());
        }
        if ((msg.sourceVertexId != getVertexId().get() - 1) &&
            (getVertexId().get() != 0)) {
          throw new IllegalStateException(
              "compute: Impossible that this message didn't come " +
                  "from the previous vertex and came from " +
                  msg.sourceVertexId);
        }
      }
      int vertexValue = getVertexValue().get();
      setVertexValue(new IntWritable(vertexValue + (int) msgValue));
      if (LOG.isDebugEnabled()) {
        LOG.debug("compute: vertex " + getVertexId() +
            " has value " + getVertexValue() +
            " on superstep " + getSuperstep());
      }
      for (LongWritable targetVertexId : this) {
        FloatWritable edgeValue = getEdgeValue(targetVertexId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("compute: vertex " + getVertexId() +
              " sending edgeValue " + edgeValue +
              " vertexValue " + vertexValue +
              " total " +
              (edgeValue.get() + (float) vertexValue) +
              " to vertex " + targetVertexId +
              " on superstep " + getSuperstep());
        }
        edgeValue.set(edgeValue.get() + (float) vertexValue);
        addEdge(targetVertexId, edgeValue);
        sendMsg(targetVertexId,
            new VerifiableMessage(
                getSuperstep(), getVertexId().get(), edgeValue.get()));
      }
    }
  }
}
