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

import org.apache.giraph.graph.*;
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
    public static class VerifiableMessage implements Writable {
        /** Superstep sent on */
        public long superstep;
        /** Source vertex id */
        public long sourceVertexId;
        /** Value */
        public float value;

        public VerifiableMessage() {}

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

    public static class VerifyMessageVertex extends
            Vertex<LongWritable, IntWritable, FloatWritable, VerifiableMessage> {
        /** User can access this after the application finishes if local */
        public static long finalSum;
        /** Number of supersteps to run (6 by default) */
        private static int supersteps = 6;
        /** Class logger */
        private static Logger LOG = Logger.getLogger(VerifyMessageVertex.class);

        /** Dynamically set number of supersteps */
        public static final String SUPERSTEP_COUNT =
            "verifyMessageVertex.superstepCount";

        public static class VerifyMessageVertexWorkerContext extends
                WorkerContext {
            @Override
            public void preApplication() throws InstantiationException,
                    IllegalAccessException {
                registerAggregator(LongSumAggregator.class.getName(),
                    LongSumAggregator.class);
                LongSumAggregator sumAggregator = (LongSumAggregator)
                    getAggregator(LongSumAggregator.class.getName());
                sumAggregator.setAggregatedValue(new LongWritable(0));
                supersteps = getContext().getConfiguration().getInt(
                    SUPERSTEP_COUNT, supersteps);
            }

            @Override
            public void postApplication() {
                LongSumAggregator sumAggregator = (LongSumAggregator)
                    getAggregator(LongSumAggregator.class.getName());
                finalSum = sumAggregator.getAggregatedValue().get();
            }

            @Override
            public void preSuperstep() {
                useAggregator(LongSumAggregator.class.getName());
            }

            @Override
            public void postSuperstep() {}
        }

        @Override
        public void compute(Iterator<VerifiableMessage> msgIterator) {
            LongSumAggregator sumAggregator = (LongSumAggregator)
                getAggregator(LongSumAggregator.class.getName());
            if (getSuperstep() > supersteps) {
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
