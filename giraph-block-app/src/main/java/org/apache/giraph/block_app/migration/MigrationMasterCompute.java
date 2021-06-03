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
package org.apache.giraph.block_app.migration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.StatusReporter;
import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullAbstractComputation;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;

/**
 * Replacement for MasterCompute when migrating to Blocks Framework,
 * disallowing functions that are tied to execution order.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class MigrationMasterCompute
    extends DefaultImmutableClassesGiraphConfigurable implements Writable {
  private BlockMasterApi api;

  final void init(BlockMasterApi masterApi) {
    this.api = masterApi;
    setConf(masterApi.getConf());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }

  public void compute() {
  }

  public void initialize() throws InstantiationException,
      IllegalAccessException {
  }

  @SuppressWarnings("deprecation")
  public long getTotalNumVertices() {
    return api.getTotalNumVertices();
  }

  @SuppressWarnings("deprecation")
  public long getTotalNumEdges() {
    return api.getTotalNumEdges();
  }

  public final <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp) {
    api.registerReducer(name, reduceOp);
  }

  public final <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp, R globalInitialValue) {
    api.registerReducer(
        name, reduceOp, globalInitialValue);
  }

  public final <T extends Writable> T getReduced(String name) {
    return api.getReduced(name);
  }

  public final void broadcast(String name, Writable object) {
    api.broadcast(name, object);
  }

  public final <A extends Writable> boolean registerAggregator(
    String name, Class<? extends Aggregator<A>> aggregatorClass)
    throws InstantiationException, IllegalAccessException {
    return api.registerAggregator(
        name, aggregatorClass);
  }

  @SuppressWarnings("deprecation")
  public final <A extends Writable> boolean registerPersistentAggregator(
      String name,
      Class<? extends Aggregator<A>> aggregatorClass) throws
      InstantiationException, IllegalAccessException {
    return api.registerPersistentAggregator(name, aggregatorClass);
  }

  public final <A extends Writable> A getAggregatedValue(String name) {
    return api.<A>getAggregatedValue(name);
  }

  public final <A extends Writable> void setAggregatedValue(
      String name, A value) {
    api.setAggregatedValue(name, value);
  }

  public final void logToCommandLine(String line) {
    api.logToCommandLine(line);
  }

  public final StatusReporter getContext() {
    return api;
  }

  /**
   * Drop-in replacement for MasterCompute when migrating
   * to Blocks Framework.
   */
  public static class MigrationFullMasterCompute
      extends MigrationMasterCompute {
    private long superstep;
    private boolean halt;
    private Class<? extends MigrationAbstractComputation> computationClass;
    private Class<? extends MigrationAbstractComputation> newComputationClass;
    private Class<? extends Writable> originalMessage;
    private Class<? extends Writable> newMessage;
    private Class<? extends MessageCombiner> originalMessageCombiner;
    private Class<? extends MessageCombiner> newMessageCombiner;

    final void init(
        long superstep,
        Class<? extends MigrationAbstractComputation> computationClass,
        Class<? extends Writable> message,
        Class<? extends MessageCombiner> messageCombiner) {
      this.superstep = superstep;
      this.halt = false;
      this.computationClass = computationClass;
      this.newComputationClass = null;
      this.originalMessage = message;
      this.newMessage = null;
      this.originalMessageCombiner = messageCombiner;
      this.newMessageCombiner = null;
    }

    public final long getSuperstep() {
      return superstep;
    }

    @Override
    public final long getTotalNumVertices() {
      if (superstep == 0) {
        throw new RuntimeException(
            "getTotalNumVertices not available in superstep=0");
      }
      return super.getTotalNumVertices();
    }

    @Override
    public final long getTotalNumEdges() {
      if (superstep == 0) {
        throw new RuntimeException(
            "getTotalNumEdges not available in superstep=0");
      }
      return super.getTotalNumEdges();
    }


    public final void haltComputation() {
      halt = true;
    }

    public final boolean isHalted() {
      return halt;
    }

    public final void setComputation(
        Class<? extends MigrationFullAbstractComputation> computation) {
      if (computation != null) {
        newComputationClass = computation;
      } else {
        // TODO
        this.computationClass = null;
      }
    }

    public final
    Class<? extends MigrationAbstractComputation> getComputation() {
      if (newComputationClass != null) {
        return newComputationClass;
      }
      if (computationClass != null) {
        return computationClass;
      }
      return null;
    }

    public final void setMessageCombiner(
        Class<? extends MessageCombiner> combinerClass) {
      this.newMessageCombiner = combinerClass;
    }

    public final Class<? extends MessageCombiner> getMessageCombiner() {
      return newMessageCombiner != null ?
        newMessageCombiner : originalMessageCombiner;
    }

    public final void setIncomingMessage(
        Class<? extends Writable> incomingMessageClass) {
      if (!originalMessage.equals(incomingMessageClass)) {
        throw new IllegalArgumentException(
            originalMessage + " and " + incomingMessageClass + " must be same");
      }
    }

    public final void setOutgoingMessage(
        Class<? extends Writable> outgoingMessageClass) {
      newMessage = outgoingMessageClass;
    }

    final Class<? extends Writable> getOutgoingMessage() {
      if (newMessage != null) {
        return newMessage;
      }

      if (newComputationClass == null) {
        return originalMessage;
      }
      Class[] computationTypes = ReflectionUtils.getTypeArguments(
          TypesHolder.class, newComputationClass);
      return computationTypes[4];
    }

    final Class<? extends MigrationAbstractComputation> getComputationClass() {
      return newComputationClass != null ?
        newComputationClass : computationClass;
    }

    final
    Class<? extends MigrationAbstractComputation> getNewComputationClass() {
      return newComputationClass;
    }

    final Class<? extends Writable> getNewMessage() {
      return newMessage;
    }

    final Class<? extends MessageCombiner> getNewMessageCombiner() {
      return newMessageCombiner;
    }
  }
}
