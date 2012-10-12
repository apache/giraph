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

package org.apache.giraph.graph;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Worker implementation of {@link AggregatorHandler}
 */
public class WorkerAggregatorHandler extends AggregatorHandler implements
    WorkerAggregatorUsage {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(WorkerAggregatorHandler.class);

  @Override
  public <A extends Writable> void aggregate(String name, A value) {
    AggregatorWrapper<? extends Writable> aggregator = getAggregator(name);
    if (aggregator != null) {
      ((AggregatorWrapper<A>) aggregator).aggregateCurrent(value);
    } else {
      throw new IllegalStateException("aggregate: Tried to aggregate value " +
          "to unregistered aggregator " + name);
    }
  }

  /**
   * Get aggregator values aggregated by master in previous superstep
   *
   * @param superstep Superstep which we are preparing for
   * @param service BspService to get zookeeper info from
   */
  public void prepareSuperstep(long superstep, BspService service) {
    // prepare aggregators for reading and next superstep
    for (AggregatorWrapper<Writable> aggregator :
        getAggregatorMap().values()) {
      aggregator.setPreviousAggregatedValue(aggregator.createInitialValue());
      aggregator.resetCurrentAggregator();
    }
    String mergedAggregatorPath =
        service.getMergedAggregatorPath(service.getApplicationAttempt(),
            superstep - 1);

    byte[] aggregatorArray;
    try {
      aggregatorArray =
          service.getZkExt().getData(mergedAggregatorPath, false, null);
    } catch (KeeperException.NoNodeException e) {
      LOG.info("getAggregatorValues: no aggregators in " +
          mergedAggregatorPath + " on superstep " + superstep);
      return;
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to get data for " +
          mergedAggregatorPath + " with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Failed to get data for " +
          mergedAggregatorPath + " with InterruptedException", e);
    }

    DataInput input =
        new DataInputStream(new ByteArrayInputStream(aggregatorArray));
    int numAggregators = 0;

    try {
      numAggregators = input.readInt();
    } catch (IOException e) {
      throw new IllegalStateException("getAggregatorValues: " +
          "Failed to decode data", e);
    }

    for (int i = 0; i < numAggregators; i++) {
      try {
        String aggregatorName = input.readUTF();
        String aggregatorClassName = input.readUTF();
        AggregatorWrapper<Writable> aggregator =
            registerAggregator(aggregatorName, aggregatorClassName, false);
        Writable aggregatorValue = aggregator.createInitialValue();
        aggregatorValue.readFields(input);
        aggregator.setPreviousAggregatedValue(aggregatorValue);
      } catch (IOException e) {
        throw new IllegalStateException(
            "Failed to decode data for index " + i, e);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("getAggregatorValues: Finished loading " +
          mergedAggregatorPath);
    }
  }

  /**
   * Put aggregator values of the worker to a byte array that will later be
   * aggregated by master.
   *
   * @param superstep Superstep which we are finishing.
   * @return Byte array of the aggreagtor values
   */
  public byte[] finishSuperstep(long superstep) {
    if (superstep == BspService.INPUT_SUPERSTEP) {
      return new byte[0];
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(outputStream);
    for (Map.Entry<String, AggregatorWrapper<Writable>> entry :
        getAggregatorMap().entrySet()) {
      if (entry.getValue().isChanged()) {
        try {
          output.writeUTF(entry.getKey());
          entry.getValue().getCurrentAggregatedValue().write(output);
        } catch (IOException e) {
          throw new IllegalStateException("Failed to marshall aggregator " +
              "with IOException " + entry.getKey(), e);
        }
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info(
          "marshalAggregatorValues: Finished assembling aggregator values");
    }
    return outputStream.toByteArray();
  }
}
