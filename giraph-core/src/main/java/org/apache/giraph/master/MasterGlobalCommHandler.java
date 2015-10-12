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

import org.apache.giraph.master.input.MasterInputSplitsHandler;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.Writable;

/**
 * Handler for all master communications
 */
public class MasterGlobalCommHandler implements MasterGlobalCommUsage {
  /** Aggregator handler */
  private final MasterAggregatorHandler aggregatorHandler;
  /** Input splits handler*/
  private final MasterInputSplitsHandler inputSplitsHandler;

  /**
   * Constructor
   *
   * @param aggregatorHandler Aggregator handler
   * @param inputSplitsHandler Input splits handler
   */
  public MasterGlobalCommHandler(
      MasterAggregatorHandler aggregatorHandler,
      MasterInputSplitsHandler inputSplitsHandler) {
    this.aggregatorHandler = aggregatorHandler;
    this.inputSplitsHandler = inputSplitsHandler;
  }

  public MasterAggregatorHandler getAggregatorHandler() {
    return aggregatorHandler;
  }

  public MasterInputSplitsHandler getInputSplitsHandler() {
    return inputSplitsHandler;
  }

  @Override
  public <S, R extends Writable> void registerReducer(String name,
      ReduceOperation<S, R> reduceOp) {
    aggregatorHandler.registerReducer(name, reduceOp);
  }

  @Override
  public <S, R extends Writable> void registerReducer(String name,
      ReduceOperation<S, R> reduceOp, R globalInitialValue) {
    aggregatorHandler.registerReducer(name, reduceOp, globalInitialValue);
  }

  @Override
  public <R extends Writable> R getReduced(String name) {
    return aggregatorHandler.getReduced(name);
  }

  @Override
  public void broadcast(String name, Writable value) {
    aggregatorHandler.broadcast(name, value);
  }
}
