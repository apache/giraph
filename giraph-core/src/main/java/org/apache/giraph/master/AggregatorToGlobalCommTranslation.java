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
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.comm.aggregators.AggregatorUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.MasterLoggingAggregator;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Class that translates aggregator handling on the master to
 * reduce and broadcast operations supported by the MasterAggregatorHandler.
 */
public class AggregatorToGlobalCommTranslation
    implements MasterAggregatorUsage, Writable {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(AggregatorToGlobalCommTranslation.class);

  /** Class providing reduce and broadcast interface to use */
  private final MasterGlobalCommUsage globalComm;
  /** List of registered aggregators */
  private final HashMap<String, AggregatorWrapper<Writable>>
  registeredAggregators = new HashMap<>();

  /**
   * List of init aggregator values, in case someone tries to
   * access aggregator immediatelly after registering it.
   *
   * Instead of simply returning value, we need to store it during
   * that superstep, so consecutive calls will return identical object,
   * which they can modify.
   */
  private final HashMap<String, Writable>
  initAggregatorValues = new HashMap<>();

  /** Conf */
  private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;

  /**
   * Constructor
   * @param conf Configuration
   * @param globalComm Global communication interface
   */
  public AggregatorToGlobalCommTranslation(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf,
      MasterGlobalCommUsage globalComm) {
    this.conf = conf;
    this.globalComm = globalComm;
    MasterLoggingAggregator.registerAggregator(this, conf);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    AggregatorWrapper<Writable> agg = registeredAggregators.get(name);
    if (agg == null) {
      LOG.warn("getAggregatedValue: " +
        AggregatorUtils.getUnregisteredAggregatorMessage(name,
            registeredAggregators.size() != 0, conf));
      // to make sure we are not accessing reducer of the same name.
      return null;
    }

    A value = globalComm.getReduced(name);
    if (value == null) {
      value = (A) initAggregatorValues.get(name);
    }

    if (value == null) {
      value = (A) agg.getReduceOp().createInitialValue();
      initAggregatorValues.put(name, value);
    }

    Preconditions.checkState(value != null);
    return value;
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    AggregatorWrapper<Writable> aggregator = registeredAggregators.get(name);
    if (aggregator == null) {
      throw new IllegalArgumentException("setAggregatedValue: "  +
          AggregatorUtils.getUnregisteredAggregatorMessage(name,
              registeredAggregators.size() != 0, conf));
    }
    aggregator.setCurrentValue(value);
  }

  /**
   * Called after master compute, to do aggregator-&gt;reduce/broadcast
   * translation
   */
  public void postMasterCompute() {
    // broadcast what master set, or if it didn't broadcast reduced value
    // register reduce with the same value
    for (Entry<String, AggregatorWrapper<Writable>> entry :
        registeredAggregators.entrySet()) {
      Writable value = entry.getValue().getCurrentValue();
      if (value == null) {
        value = globalComm.getReduced(entry.getKey());
      }
      Preconditions.checkState(value != null);

      globalComm.broadcast(entry.getKey(), new AggregatorBroadcast<>(
          entry.getValue().getReduceOp().getAggregatorClass(), value));

      // Always register clean instance of reduceOp, not to conflict with
      // reduceOp from previous superstep.
      AggregatorReduceOperation<Writable> cleanReduceOp =
          entry.getValue().createReduceOp();
      if (entry.getValue().isPersistent()) {
        globalComm.registerReducer(
            entry.getKey(), cleanReduceOp, value);
      } else {
        globalComm.registerReducer(
            entry.getKey(), cleanReduceOp);
      }
      entry.getValue().setCurrentValue(null);
    }
    initAggregatorValues.clear();
  }

  /** Prepare before calling master compute */
  public void prepareSuperstep() {
    MasterLoggingAggregator.logAggregatedValue(this, conf);
  }

  @Override
  public <A extends Writable> boolean registerAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    registerAggregator(name, aggregatorClass, false);
    return true;
  }

  @Override
  public <A extends Writable> boolean registerPersistentAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    registerAggregator(name, aggregatorClass, true);
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(registeredAggregators.size());
    for (Entry<String, AggregatorWrapper<Writable>> entry :
        registeredAggregators.entrySet()) {
      out.writeUTF(entry.getKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    registeredAggregators.clear();
    int numAggregators = in.readInt();
    for (int i = 0; i < numAggregators; i++) {
      String name = in.readUTF();
      AggregatorWrapper<Writable> agg = new AggregatorWrapper<>();
      agg.readFields(in);
      registeredAggregators.put(name, agg);
    }
    initAggregatorValues.clear();
  }

  /**
   * Helper function for registering aggregators.
   *
   * @param name            Name of the aggregator
   * @param aggregatorClass Aggregator class
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
        (AggregatorWrapper<A>) registeredAggregators.get(name);
    if (aggregatorWrapper == null) {
      aggregatorWrapper =
          new AggregatorWrapper<A>(aggregatorClass, persistent);
      // postMasterCompute uses previously reduced value to broadcast,
      // unless current value is set. After aggregator is registered,
      // there was no previously reduced value, so set current value
      // to default to avoid calling getReduced() on unregistered reducer.
      // (which logs unnecessary warnings)
      aggregatorWrapper.setCurrentValue(
          aggregatorWrapper.getReduceOp().createInitialValue());
      registeredAggregators.put(
          name, (AggregatorWrapper<Writable>) aggregatorWrapper);
    }
    return aggregatorWrapper;
  }

  /**
   * Object holding all needed data related to single Aggregator
   * @param <A> Aggregated value type
   */
  private class AggregatorWrapper<A extends Writable>
      implements Writable {
    /** False iff aggregator should be reset at the end of each super step */
    private boolean persistent;
    /** Translation of aggregator to reduce operations */
    private AggregatorReduceOperation<A> reduceOp;
    /** Current value, set by master manually */
    private A currentValue;

    /** Constructor */
    public AggregatorWrapper() {
    }

    /**
     * Constructor
     * @param aggregatorClass Aggregator class
     * @param persistent Is persistent
     */
    public AggregatorWrapper(
        Class<? extends Aggregator<A>> aggregatorClass,
        boolean persistent) {
      this.persistent = persistent;
      this.reduceOp = new AggregatorReduceOperation<>(aggregatorClass, conf);
    }

    public AggregatorReduceOperation<A> getReduceOp() {
      return reduceOp;
    }

    /**
     * Create a fresh instance of AggregatorReduceOperation
     * @return fresh instance of AggregatorReduceOperation
     */
    public AggregatorReduceOperation<A> createReduceOp() {
      return reduceOp.createCopy();
    }

    public A getCurrentValue() {
      return currentValue;
    }

    public void setCurrentValue(A currentValue) {
      this.currentValue = currentValue;
    }

    public boolean isPersistent() {
      return persistent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(persistent);
      reduceOp.write(out);

      Preconditions.checkState(currentValue == null, "AggregatorWrapper " +
          "shouldn't have value at the end of the superstep");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      persistent = in.readBoolean();
      reduceOp = WritableUtils.createWritable(
          AggregatorReduceOperation.class, conf);
      reduceOp.readFields(in);
      currentValue = null;
    }
  }
}
