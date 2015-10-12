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
package org.apache.giraph.master;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.reducers.Reducer;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/** Handler for reduce/broadcast on the master */
public class MasterAggregatorHandler
    implements MasterGlobalCommUsageAggregators, Writable {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MasterAggregatorHandler.class);

  /** Map of reducers registered for the next worker computation */
  private final Map<String, Reducer<Object, Writable>> reducerMap =
      Maps.newHashMap();
  /** Map of values to be sent to workers for next computation */
  private final Map<String, Writable> broadcastMap =
      Maps.newHashMap();
  /** Values reduced from previous computation */
  private final Map<String, Writable> reducedMap =
      Maps.newHashMap();

  /** Aggregator writer - for writing reduced values */
  private final AggregatorWriter aggregatorWriter;
  /** Progressable used to report progress */
  private final Progressable progressable;

  /** Conf */
  private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param progressable Progress reporter
   */
  public MasterAggregatorHandler(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
      Progressable progressable) {
    this.progressable = progressable;
    this.conf = conf;
    aggregatorWriter = conf.createAggregatorWriter();
  }

  @Override
  public final <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp) {
    registerReducer(name, reduceOp, reduceOp.createInitialValue());
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp,
      R globalInitialValue) {
    if (reducerMap.containsKey(name)) {
      throw new IllegalArgumentException(
          "Reducer with name " + name + " was already registered, " +
          " and is " + reducerMap.get(name) + ", and we are trying to " +
          " register " + reduceOp);
    }
    if (reduceOp == null) {
      throw new IllegalArgumentException(
          "null reducer cannot be registered, with name " + name);
    }
    if (globalInitialValue == null) {
      throw new IllegalArgumentException(
          "global initial value for reducer cannot be null, but is for " +
          reduceOp + " with naem" + name);
    }

    Reducer<S, R> reducer = new Reducer<>(reduceOp, globalInitialValue);
    reducerMap.put(name, (Reducer<Object, Writable>) reducer);
  }

  @Override
  public <T extends Writable> T getReduced(String name) {
    T value = (T) reducedMap.get(name);
    if (value == null) {
      LOG.warn("getReduced: " +
        AggregatorUtils.getUnregisteredReducerMessage(name,
            reducedMap.size() != 0, conf));
    }
    return value;
  }

  @Override
  public void broadcast(String name, Writable object) {
    if (broadcastMap.containsKey(name)) {
      throw new IllegalArgumentException(
          "Value already broadcasted for name " + name);
    }
    if (object == null) {
      throw new IllegalArgumentException("null cannot be broadcasted");
    }

    broadcastMap.put(name, object);
  }

  /** Prepare reduced values for current superstep's master compute */
  public void prepareSuperstep() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Start preparing reducers");
    }

    Preconditions.checkState(reducedMap.isEmpty(),
        "reducedMap must be empty before start of the superstep");
    Preconditions.checkState(broadcastMap.isEmpty(),
        "broadcastMap must be empty before start of the superstep");

    for (Entry<String, Reducer<Object, Writable>> entry :
        reducerMap.entrySet()) {
      Writable value = entry.getValue().getCurrentValue();
      if (value == null) {
        value = entry.getValue().createInitialValue();
      }

      reducedMap.put(entry.getKey(), value);
    }

    reducerMap.clear();

    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Aggregators prepared");
    }
  }

  /** Finalize aggregators for current superstep */
  public void finishSuperstep() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("finishSuperstep: Start finishing aggregators");
    }

    reducedMap.clear();

    if (LOG.isDebugEnabled()) {
      LOG.debug("finishSuperstep: Aggregators finished");
    }
  }

  /**
   * Send data to workers (through owner workers)
   *
   * @param masterClient IPC client on master
   */
  public void sendDataToOwners(MasterClient masterClient) {
    // send broadcast values and reduceOperations to their owners
    try {
      for (Entry<String, Reducer<Object, Writable>> entry :
          reducerMap.entrySet()) {
        masterClient.sendToOwner(entry.getKey(),
            GlobalCommType.REDUCE_OPERATIONS,
            entry.getValue().getReduceOp());
        progressable.progress();
      }

      for (Entry<String, Writable> entry : broadcastMap.entrySet()) {
        masterClient.sendToOwner(entry.getKey(),
            GlobalCommType.BROADCAST,
            entry.getValue());
        progressable.progress();
      }
      masterClient.finishSendingValues();

      broadcastMap.clear();
    } catch (IOException e) {
      throw new IllegalStateException("finishSuperstep: " +
          "IOException occurred while sending aggregators", e);
    }
  }

  /**
   * Accept reduced values sent by worker. Every value will be sent
   * only once, by its owner.
   * We don't need to count the number of these requests because global
   * superstep barrier will happen after workers ensure all requests of this
   * type have been received and processed by master.
   *
   * @param reducedValuesInput Input in which aggregated values are
   *                              written in the following format:
   *                              numReducers
   *                              name_1  REDUCED_VALUE  value_1
   *                              name_2  REDUCED_VALUE  value_2
   *                              ...
   * @throws IOException
   */
  public void acceptReducedValues(
      DataInput reducedValuesInput) throws IOException {
    int numReducers = reducedValuesInput.readInt();
    for (int i = 0; i < numReducers; i++) {
      String name = reducedValuesInput.readUTF();
      GlobalCommType type =
          GlobalCommType.values()[reducedValuesInput.readByte()];
      if (type != GlobalCommType.REDUCED_VALUE) {
        throw new IllegalStateException(
            "SendReducedToMasterRequest received " + type);
      }
      Reducer<Object, Writable> reducer = reducerMap.get(name);
      if (reducer == null) {
        throw new IllegalStateException(
            "acceptReducedValues: " +
                "Master received reduced value which isn't registered: " +
                name);
      }

      Writable valueToReduce = reducer.createInitialValue();
      valueToReduce.readFields(reducedValuesInput);

      if (reducer.getCurrentValue() != null) {
        reducer.reduceMerge(valueToReduce);
      } else {
        reducer.setCurrentValue(valueToReduce);
      }
      progressable.progress();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("acceptReducedValues: Accepted one set with " +
          numReducers + " aggregated values");
    }
  }

  /**
   * Write aggregators to {@link AggregatorWriter}
   *
   * @param superstep      Superstep which just finished
   * @param superstepState State of the superstep which just finished
   */
  public void writeAggregators(
      long superstep, SuperstepState superstepState) {
    try {
      aggregatorWriter.writeAggregator(reducedMap.entrySet(),
          (superstepState == SuperstepState.ALL_SUPERSTEPS_DONE) ?
              AggregatorWriter.LAST_SUPERSTEP : superstep);
    } catch (IOException e) {
      throw new IllegalStateException(
          "coordinateSuperstep: IOException while " +
              "writing aggregators data", e);
    }
  }

  /**
   * Initialize {@link AggregatorWriter}
   *
   * @param service BspService
   */
  public void initialize(BspService service) {
    try {
      aggregatorWriter.initialize(service.getContext(),
          service.getApplicationAttempt());
    } catch (IOException e) {
      throw new IllegalStateException("initialize: " +
          "Couldn't initialize aggregatorWriter", e);
    }
  }

  /**
   * Close {@link AggregatorWriter}
   *
   * @throws IOException
   */
  public void close() throws IOException {
    aggregatorWriter.close();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // At the end of superstep, only reduceOpMap can be non-empty
    Preconditions.checkState(reducedMap.isEmpty(),
        "reducedMap must be empty at the end of the superstep");

    out.writeInt(reducerMap.size());
    for (Entry<String, Reducer<Object, Writable>> entry :
        reducerMap.entrySet()) {
      out.writeUTF(entry.getKey());
      entry.getValue().write(out);
      progressable.progress();
    }

    out.writeInt(broadcastMap.size());
    for (Entry<String, Writable> entry : broadcastMap.entrySet()) {
      out.writeUTF(entry.getKey());
      WritableUtils.writeWritableObject(entry.getValue(), out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reducedMap.clear();
    broadcastMap.clear();
    reducerMap.clear();

    int numReducers = in.readInt();
    for (int i = 0; i < numReducers; i++) {
      String name = in.readUTF();
      Reducer<Object, Writable> reducer = new Reducer<>();
      reducer.readFields(in, conf);
      reducerMap.put(name, reducer);
    }

    int numBroadcast = in.readInt();
    for (int i = 0; i < numBroadcast; i++) {
      String name = in.readUTF();
      Writable value = WritableUtils.readWritableObject(in, conf);
      broadcastMap.put(name, value);
    }
  }
}
