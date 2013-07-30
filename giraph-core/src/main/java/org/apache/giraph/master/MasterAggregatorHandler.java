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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.SuperstepState;
import org.apache.giraph.comm.MasterClient;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.AggregatorWrapper;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.utils.MasterLoggingAggregator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

/** Handler for aggregators on master */
public class MasterAggregatorHandler implements MasterAggregatorUsage,
    Writable {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MasterAggregatorHandler.class);
  /**
   * Map of aggregators.
   * This map is used to store final aggregated values received from worker
   * owners, and also to read and write values provided during master.compute.
   */
  private final Map<String, AggregatorWrapper<Writable>> aggregatorMap =
      Maps.newHashMap();
  /** Aggregator writer */
  private final AggregatorWriter aggregatorWriter;
  /** Progressable used to report progress */
  private final Progressable progressable;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

  /**
   * Constructor
   *
   * @param conf         Giraph configuration
   * @param progressable Progressable used for reporting progress
   */
  public MasterAggregatorHandler(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
      Progressable progressable) {
    this.conf = conf;
    this.progressable = progressable;
    aggregatorWriter = conf.createAggregatorWriter();
    MasterLoggingAggregator.registerAggregator(this, conf);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    AggregatorWrapper<? extends Writable> aggregator = aggregatorMap.get(name);
    if (aggregator == null) {
      LOG.warn("getAggregatedValue: " +
          AggregatorUtils.getUnregisteredAggregatorMessage(name,
              aggregatorMap.size() != 0, conf));
      return null;
    } else {
      return (A) aggregator.getPreviousAggregatedValue();
    }
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    AggregatorWrapper<? extends Writable> aggregator = aggregatorMap.get(name);
    if (aggregator == null) {
      throw new IllegalStateException(
          "setAggregatedValue: " +
              AggregatorUtils.getUnregisteredAggregatorMessage(name,
                  aggregatorMap.size() != 0, conf));
    }
    ((AggregatorWrapper<A>) aggregator).setCurrentAggregatedValue(value);
  }

  @Override
  public <A extends Writable> boolean registerAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    checkAggregatorName(name);
    return registerAggregator(name, aggregatorClass, false) != null;
  }

  @Override
  public <A extends Writable> boolean registerPersistentAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    checkAggregatorName(name);
    return registerAggregator(name, aggregatorClass, true) != null;
  }

  /**
   * Make sure user doesn't use AggregatorUtils.SPECIAL_COUNT_AGGREGATOR as
   * the name of aggregator. Throw an exception if he tries to use it.
   *
   * @param name Name of the aggregator to check.
   */
  private void checkAggregatorName(String name) {
    if (name.equals(AggregatorUtils.SPECIAL_COUNT_AGGREGATOR)) {
      throw new IllegalStateException("checkAggregatorName: " +
          AggregatorUtils.SPECIAL_COUNT_AGGREGATOR +
          " is not allowed for the name of aggregator");
    }
  }

  /**
   * Helper function for registering aggregators.
   *
   * @param name            Name of the aggregator
   * @param aggregatorClass Class of the aggregator
   * @param persistent      Whether aggregator is persistent or not
   * @param <A>             Aggregated value type
   * @return Newly registered aggregator or aggregator which was previously
   *         created with selected name, if any
   */
  private <A extends Writable> AggregatorWrapper<A> registerAggregator
  (String name, Class<? extends Aggregator<A>> aggregatorClass,
      boolean persistent) throws InstantiationException,
      IllegalAccessException {
    AggregatorWrapper<A> aggregatorWrapper =
        (AggregatorWrapper<A>) aggregatorMap.get(name);
    if (aggregatorWrapper == null) {
      aggregatorWrapper =
          new AggregatorWrapper<A>(aggregatorClass, persistent, conf);
      aggregatorMap.put(name, (AggregatorWrapper<Writable>) aggregatorWrapper);
    }
    return aggregatorWrapper;
  }

  /**
   * Prepare aggregators for current superstep
   *
   * @param masterClient IPC client on master
   */
  public void prepareSuperstep(MasterClient masterClient) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Start preparing aggregators");
    }
    // prepare aggregators for master compute
    for (AggregatorWrapper<Writable> aggregator : aggregatorMap.values()) {
      if (aggregator.isPersistent()) {
        aggregator.aggregateCurrent(aggregator.getPreviousAggregatedValue());
      }
      aggregator.setPreviousAggregatedValue(
          aggregator.getCurrentAggregatedValue());
      aggregator.resetCurrentAggregator();
      progressable.progress();
    }
    MasterLoggingAggregator.logAggregatedValue(this, conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareSuperstep: Aggregators prepared");
    }
  }

  /**
   * Finalize aggregators for current superstep and share them with workers
   *
   * @param masterClient IPC client on master
   */
  public void finishSuperstep(MasterClient masterClient) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("finishSuperstep: Start finishing aggregators");
    }
    for (AggregatorWrapper<Writable> aggregator : aggregatorMap.values()) {
      if (aggregator.isChanged()) {
        // if master compute changed the value, use the one he chose
        aggregator.setPreviousAggregatedValue(
            aggregator.getCurrentAggregatedValue());
        // reset aggregator for the next superstep
        aggregator.resetCurrentAggregator();
      }
      progressable.progress();
    }

    // send aggregators to their owners
    // TODO: if aggregator owner and it's value didn't change,
    //       we don't need to resend it
    try {
      for (Map.Entry<String, AggregatorWrapper<Writable>> entry :
          aggregatorMap.entrySet()) {
        masterClient.sendAggregator(entry.getKey(),
            entry.getValue().getAggregatorClass(),
            entry.getValue().getPreviousAggregatedValue());
        progressable.progress();
      }
      masterClient.finishSendingAggregatedValues();
    } catch (IOException e) {
      throw new IllegalStateException("finishSuperstep: " +
          "IOException occurred while sending aggregators", e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("finishSuperstep: Aggregators finished");
    }
  }

  /**
   * Accept aggregated values sent by worker. Every aggregator will be sent
   * only once, by its owner.
   * We don't need to count the number of these requests because global
   * superstep barrier will happen after workers ensure all requests of this
   * type have been received and processed by master.
   *
   * @param aggregatedValuesInput Input in which aggregated values are
   *                              written in the following format:
   *                              number_of_aggregators
   *                              name_1  value_1
   *                              name_2  value_2
   *                              ...
   * @throws IOException
   */
  public void acceptAggregatedValues(
      DataInput aggregatedValuesInput) throws IOException {
    int numAggregators = aggregatedValuesInput.readInt();
    for (int i = 0; i < numAggregators; i++) {
      String aggregatorName = aggregatedValuesInput.readUTF();
      AggregatorWrapper<Writable> aggregator =
          aggregatorMap.get(aggregatorName);
      if (aggregator == null) {
        throw new IllegalStateException(
            "acceptAggregatedValues: " +
                "Master received aggregator which isn't registered: " +
                aggregatorName);
      }
      Writable aggregatorValue = aggregator.createInitialValue();
      aggregatorValue.readFields(aggregatedValuesInput);
      aggregator.setCurrentAggregatedValue(aggregatorValue);
      progressable.progress();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("acceptAggregatedValues: Accepted one set with " +
          numAggregators + " aggregated values");
    }
  }

  /**
   * Write aggregators to {@link AggregatorWriter}
   *
   * @param superstep      Superstep which just finished
   * @param superstepState State of the superstep which just finished
   */
  public void writeAggregators(long superstep, SuperstepState superstepState) {
    try {
      Iterable<Map.Entry<String, Writable>> iter =
          Iterables.transform(
              aggregatorMap.entrySet(),
              new Function<Map.Entry<String, AggregatorWrapper<Writable>>,
                  Map.Entry<String, Writable>>() {
                @Override
                public Map.Entry<String, Writable> apply(
                    Map.Entry<String, AggregatorWrapper<Writable>> entry) {
                  progressable.progress();
                  return new AbstractMap.SimpleEntry<String,
                      Writable>(entry.getKey(),
                      entry.getValue().getPreviousAggregatedValue());
                }
              });
      aggregatorWriter.writeAggregator(iter,
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
    out.writeInt(aggregatorMap.size());
    for (Map.Entry<String, AggregatorWrapper<Writable>> entry :
        aggregatorMap.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue().getAggregatorClass().getName());
      out.writeBoolean(entry.getValue().isPersistent());
      entry.getValue().getPreviousAggregatedValue().write(out);
      progressable.progress();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    aggregatorMap.clear();
    int numAggregators = in.readInt();
    try {
      for (int i = 0; i < numAggregators; i++) {
        String aggregatorName = in.readUTF();
        String aggregatorClassName = in.readUTF();
        boolean isPersistent = in.readBoolean();
        AggregatorWrapper<Writable> aggregator = registerAggregator(
            aggregatorName,
            AggregatorUtils.getAggregatorClass(aggregatorClassName),
            isPersistent);
        Writable value = aggregator.createInitialValue();
        value.readFields(in);
        aggregator.setPreviousAggregatedValue(value);
        progressable.progress();
      }
    } catch (InstantiationException e) {
      throw new IllegalStateException("readFields: " +
          "InstantiationException occurred", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("readFields: " +
          "IllegalAccessException occurred", e);
    }
  }
}
