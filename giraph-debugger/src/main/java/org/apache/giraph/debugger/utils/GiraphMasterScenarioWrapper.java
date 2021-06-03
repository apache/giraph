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
package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.debugger.Scenario.CommonVertexMasterContext;
import org.apache.giraph.debugger.Scenario.Exception;
import org.apache.giraph.debugger.Scenario.GiraphMasterScenario;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around
 * {@link org.apache.giraph.debugger.Scenario.GiraphMasterScenario} protocol
 * buffer.
 *
 * author semihsalihoglu
 */
public class GiraphMasterScenarioWrapper extends BaseWrapper {
  /**
   * The MasterCompute class under debugging.
   */
  private String masterClassUnderTest;
  /**
   * The common wrapper instance.
   */
  private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper =
    null;
  /**
   * The exception wrapper instance.
   */
  private ExceptionWrapper exceptionWrapper = null;

  /**
   * Default constructor.
   */
  public GiraphMasterScenarioWrapper() {
  }

  /**
   * Constructor with a MasterCompute class name.
   *
   * @param masterClassUnderTest The MasterCompute class name.
   */
  public GiraphMasterScenarioWrapper(String masterClassUnderTest) {
    this.masterClassUnderTest = masterClassUnderTest;
    this.commonVertexMasterContextWrapper = new
      CommonVertexMasterContextWrapper();
    this.exceptionWrapper = null;
  }

  public String getMasterClassUnderTest() {
    return masterClassUnderTest;
  }

  public CommonVertexMasterContextWrapper getCommonVertexMasterContextWrapper()
  {
    return commonVertexMasterContextWrapper;
  }

  public void setCommonVertexMasterContextWrapper(
    CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
    this.commonVertexMasterContextWrapper = commonVertexMasterContextWrapper;
  }

  public ExceptionWrapper getExceptionWrapper() {
    return exceptionWrapper;
  }

  public void setExceptionWrapper(ExceptionWrapper exceptionWrapper) {
    this.exceptionWrapper = exceptionWrapper;
  }

  /**
   * Checks if this has an exception wrapper.
   * @return True if this has an exception wrapper.
   */
  public boolean hasExceptionWrapper() {
    return exceptionWrapper != null;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    GiraphMasterScenario.Builder giraphMasterScenarioBuilder =
      GiraphMasterScenario.newBuilder();
    giraphMasterScenarioBuilder.setMasterClassUnderTest(masterClassUnderTest);
    giraphMasterScenarioBuilder
      .setCommonContext((CommonVertexMasterContext)
        commonVertexMasterContextWrapper.buildProtoObject());
    if (hasExceptionWrapper()) {
      giraphMasterScenarioBuilder.setException((Exception) exceptionWrapper
        .buildProtoObject());
    }
    return giraphMasterScenarioBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return GiraphMasterScenario.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    GiraphMasterScenario giraphMasterScenario = (GiraphMasterScenario)
      protoObject;
    this.masterClassUnderTest = giraphMasterScenario.getMasterClassUnderTest();
    this.commonVertexMasterContextWrapper = new
      CommonVertexMasterContextWrapper();
    this.commonVertexMasterContextWrapper.loadFromProto(giraphMasterScenario
      .getCommonContext());
    if (giraphMasterScenario.hasException()) {
      this.exceptionWrapper = new ExceptionWrapper();
      this.exceptionWrapper.loadFromProto(giraphMasterScenario.getException());
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("masterClassUnderTest: " + masterClassUnderTest);
    stringBuilder.append("\n" + commonVertexMasterContextWrapper.toString());
    stringBuilder.append("\nhasExceptionWrapper: " + hasExceptionWrapper());
    if (hasExceptionWrapper()) {
      stringBuilder.append("\n" + exceptionWrapper.toString());
    }
    return stringBuilder.toString();
  }
}
