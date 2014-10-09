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
import org.apache.giraph.aggregators.ClassAggregatorFactory;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.utils.WritableFactory;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Class that translates aggregator handling on the master to
 * reduce and broadcast operations supported by the MasterAggregatorHandler.
 */
public class AggregatorToGlobalCommTranslation
    extends DefaultImmutableClassesGiraphConfigurable
    implements MasterAggregatorUsage, Writable {
  /** Class providing reduce and broadcast interface to use */
  private final MasterGlobalCommUsage globalComm;
  /** List of registered aggregators */
  private final HashMap<String, AggregatorWrapper<Writable>>
  registeredAggregators = new HashMap<>();

  /**
   * Constructor
   * @param globalComm Global communication interface
   */
  public AggregatorToGlobalCommTranslation(MasterGlobalCommUsage globalComm) {
    this.globalComm = globalComm;
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return globalComm.getReduced(name);
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    AggregatorWrapper<Writable> aggregator = registeredAggregators.get(name);
    aggregator.setCurrentValue(value);
  }

  /**
   * Called after master compute, to do aggregator->reduce/broadcast
   * translation
   */
  public void postMasterCompute() {
    // broadcast what master set, or if it didn't broadcast reduced value
    // register reduce with the same value
    for (Entry<String, AggregatorWrapper<Writable>> entry :
        registeredAggregators.entrySet()) {
      Writable value = entry.getValue().currentValue != null ?
          entry.getValue().getCurrentValue() :
            globalComm.getReduced(entry.getKey());
      if (value == null) {
        value = entry.getValue().getReduceOp().createInitialValue();
      }

      globalComm.broadcast(entry.getKey(), value);
      // Always register clean instance of reduceOp, not to conflict with
      // reduceOp from previous superstep.
      AggregatorReduceOperation<Writable> cleanReduceOp =
          entry.getValue().createReduceOp();
      if (entry.getValue().isPersistent()) {
        globalComm.registerReduce(
            entry.getKey(), cleanReduceOp, value);
      } else {
        globalComm.registerReduce(
            entry.getKey(), cleanReduceOp);
      }
      entry.getValue().setCurrentValue(null);
    }
  }

  @Override
  public <A extends Writable> boolean registerAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    ClassAggregatorFactory<A> aggregatorFactory =
        new ClassAggregatorFactory<A>(aggregatorClass);
    return registerAggregator(name, aggregatorFactory, false) != null;
  }

  @Override
  public <A extends Writable> boolean registerAggregator(String name,
      WritableFactory<? extends Aggregator<A>> aggregator) throws
      InstantiationException, IllegalAccessException {
    return registerAggregator(name, aggregator, false) != null;
  }

  @Override
  public <A extends Writable> boolean registerPersistentAggregator(String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    ClassAggregatorFactory<A> aggregatorFactory =
        new ClassAggregatorFactory<A>(aggregatorClass);
    return registerAggregator(name, aggregatorFactory, true) != null;
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
  }

  /**
   * Helper function for registering aggregators.
   *
   * @param name              Name of the aggregator
   * @param aggregatorFactory Aggregator factory
   * @param persistent        Whether aggregator is persistent or not
   * @param <A>               Aggregated value type
   * @return Newly registered aggregator or aggregator which was previously
   *         created with selected name, if any
   */
  private <A extends Writable> AggregatorWrapper<A> registerAggregator
  (String name, WritableFactory<? extends Aggregator<A>> aggregatorFactory,
      boolean persistent) throws InstantiationException,
      IllegalAccessException {
    AggregatorWrapper<A> aggregatorWrapper =
        (AggregatorWrapper<A>) registeredAggregators.get(name);
    if (aggregatorWrapper == null) {
      aggregatorWrapper =
          new AggregatorWrapper<A>(aggregatorFactory, persistent);
      registeredAggregators.put(
          name, (AggregatorWrapper<Writable>) aggregatorWrapper);
    }
    return aggregatorWrapper;
  }

  /**
   * Object holding all needed data related to single Aggregator
   * @param <A> Aggregated value type
   */
  private static class AggregatorWrapper<A extends Writable>
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
     * @param aggregatorFactory Aggregator factory
     * @param persistent Is persistent
     */
    public AggregatorWrapper(
        WritableFactory<? extends Aggregator<A>> aggregatorFactory,
        boolean persistent) {
      this.persistent = persistent;
      this.reduceOp = new AggregatorReduceOperation<>(aggregatorFactory);
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
      reduceOp = new AggregatorReduceOperation<>();
      reduceOp.readFields(in);
      currentValue = null;
    }
  }
}
