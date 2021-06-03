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
package org.apache.giraph.debugger.instrumenter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils;
import org.apache.giraph.debugger.utils.DebuggerUtils.DebugTrace;
import org.apache.giraph.debugger.utils.ExceptionWrapper;
import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Class that intercepts calls to {@link MasterCompute}'s exposed methods for
 * GiraphDebugger.
 */
public abstract class AbstractInterceptingMasterCompute extends MasterCompute {

  /**
   * Logger for this class.
   */
  protected static final Logger LOG = Logger
    .getLogger(AbstractInterceptingMasterCompute.class);
  /**
   * The master scenario being captured.
   */
  private GiraphMasterScenarioWrapper giraphMasterScenarioWrapper;
  /**
   * The utility for intercepting master computes.
   */
  private CommonVertexMasterInterceptionUtil commonVertexMasterInterceptionUtil;

  /**
   * Called immediately as user's {@link MasterCompute#compute()} method is
   * entered.
   */
  public void interceptComputeBegin() {
    LOG.info(this.getClass().getName() + ".interceptInitializeEnd is called ");
    giraphMasterScenarioWrapper = new GiraphMasterScenarioWrapper(this
      .getClass().getName());
    if (commonVertexMasterInterceptionUtil == null) {
      commonVertexMasterInterceptionUtil = new
        CommonVertexMasterInterceptionUtil(getContext().getJobID().toString());
    }
    commonVertexMasterInterceptionUtil.initCommonVertexMasterContextWrapper(
      getConf(), getSuperstep(), getTotalNumVertices(), getTotalNumEdges());
    giraphMasterScenarioWrapper
      .setCommonVertexMasterContextWrapper(commonVertexMasterInterceptionUtil
        .getCommonVertexMasterContextWrapper());
  }

  /**
   * Intercepts the call to {@link MasterCompute#getAggregatedValue(String)} to
   * capture aggregator values at each superstep.
   *
   * @param <A>
   *          The type of the aggregator value.
   * @param name
   *          The name of the Giraph aggregator.
   * @return The aggregator value returned by the original
   *         {@link MasterCompute#getAggregatedValue(String)}.
   */
  @Intercept(renameTo = "getAggregatedValue")
  public <A extends Writable> A getAggregatedValueIntercept(String name) {
    A retVal = super.<A>getAggregatedValue(name);
    commonVertexMasterInterceptionUtil.addAggregatedValueIfNotExists(name,
      retVal);
    return retVal;
  }

  /**
   * Called when user's {@link MasterCompute#compute()} method throws an
   * exception.
   *
   * @param e
   *          exception thrown.
   */
  protected final void interceptComputeException(Exception e) {
    LOG.info("Caught an exception in user's MasterCompute. message: " +
      e.getMessage() + ". Saving a trace in HDFS.");
    ExceptionWrapper exceptionWrapper = new ExceptionWrapper(e.getMessage(),
      ExceptionUtils.getStackTrace(e));
    giraphMasterScenarioWrapper.setExceptionWrapper(exceptionWrapper);
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphMasterScenarioWrapper, DebuggerUtils.getFullMasterTraceFileName(
        DebugTrace.MASTER_EXCEPTION,
        commonVertexMasterInterceptionUtil.getJobId(), getSuperstep()));
  }

  /**
   * Called after user's {@link MasterCompute#compute()} method returns.
   */
  public void interceptComputeEnd() {
    commonVertexMasterInterceptionUtil.saveScenarioWrapper(
      giraphMasterScenarioWrapper, DebuggerUtils.getFullMasterTraceFileName(
        DebugTrace.MASTER_REGULAR,
        commonVertexMasterInterceptionUtil.getJobId(), getSuperstep()));
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
  }
}
