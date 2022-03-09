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
package org.apache.giraph.debugger.mock;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;

import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexContextWrapper.OutgoingMessageWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexScenarioClassesWrapper;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

/**
 * This is a code generator which can generate the JUnit test cases for a
 * Giraph.
 */
public class ComputationComputeTestGenerator extends TestGenerator {

  /**
   * Public constructor.
   */
  public ComputationComputeTestGenerator() {
    super();
  }

  /**
   * Generates a unit test file as a string from the given
   * {@link GiraphVertexScenarioWrapper} object.
   * @param input {@link GiraphVertexScenarioWrapper} object to generate a test
   * file from.
   * @param testPackage package name for the unit test.
   * @return a unit test file as a string from the given
   * {@link GiraphVertexScenarioWrapper} object
   */
  @SuppressWarnings("rawtypes")
  public String generateTest(GiraphVertexScenarioWrapper input,
    String testPackage) throws IOException {
    return generateTest(input, testPackage, null);
  }

  /**
   * Generates a unit test file as a string from the given
   * {@link GiraphVertexScenarioWrapper} object.
   * @param input {@link GiraphVertexScenarioWrapper} object to generate a test
   * file from.
   * @param testPackage package name for the unit test.
   * @param className name of the test class.
   * @return a unit test file as a string from the given
   * {@link GiraphVertexScenarioWrapper} object
   */
  @SuppressWarnings("rawtypes")
  public String generateTest(GiraphVertexScenarioWrapper input,
    String testPackage, String className) throws IOException {
    VelocityContext context = buildContext(input, testPackage, className);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeTestTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the classUnderTest field inside the unit test.
   */
  @SuppressWarnings("rawtypes")
  public String generateClassUnderTestField(
    GiraphVertexScenarioWrapper scenario) {
    return "private " +
      scenario.getVertexScenarioClassesWrapper().getClassUnderTest()
        .getSimpleName() + " classUnderTest;";
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the line declaring the ImmutableClassesGiraphConfiguration field
   *         inside the unit test.
   */
  @SuppressWarnings("rawtypes")
  public String generateConfField(GiraphVertexScenarioWrapper scenario) {
    return String.format(
      "private ImmutableClassesGiraphConfiguration<%s, %s, %s> conf;", scenario
        .getVertexScenarioClassesWrapper().getVertexIdClass().getSimpleName(),
      scenario.getVertexScenarioClassesWrapper().getVertexValueClass()
        .getSimpleName(), scenario.getVertexScenarioClassesWrapper()
        .getEdgeValueClass().getSimpleName());
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the line declaring the MockedEnvironment field
   *         inside the unit test.
   */
  @SuppressWarnings("rawtypes")
  public String generateMockEnvField(GiraphVertexScenarioWrapper scenario) {
    return String.format("private MockedEnvironment<%s, %s, %s, %s> mockEnv;",
      scenario.getVertexScenarioClassesWrapper().getVertexIdClass()
        .getSimpleName(), scenario.getVertexScenarioClassesWrapper()
        .getVertexValueClass().getSimpleName(), scenario
        .getVertexScenarioClassesWrapper().getEdgeValueClass().getSimpleName(),
      scenario.getVertexScenarioClassesWrapper().getOutgoingMessageClass()
        .getSimpleName());
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the line declaring the WorkerClientRequestProcessor field
   *         inside the unit test.
   */
  @SuppressWarnings("rawtypes")
  public String generateProcessorField(GiraphVertexScenarioWrapper scenario) {
    return String.format(
      "private WorkerClientRequestProcessor<%s, %s, %s> processor;", scenario
        .getVertexScenarioClassesWrapper().getVertexIdClass().getSimpleName(),
      scenario.getVertexScenarioClassesWrapper().getVertexValueClass()
        .getSimpleName(), scenario.getVertexScenarioClassesWrapper()
        .getEdgeValueClass().getSimpleName());
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the line declaring the setup method field inside the unit test.
   */
  @SuppressWarnings("rawtypes")
  public String generateSetUp(GiraphVertexScenarioWrapper scenario)
    throws IOException {
    VelocityContext context = buildContext(scenario);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeSetUpFuncTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * @param scenario scenario object from which a unit test file is being
   * generated.
   * @return the line declaring the testCompute method field inside the unit
   *         test.
   */
  @SuppressWarnings({ "rawtypes" })
  public String generateTestCompute(GiraphVertexScenarioWrapper scenario)
    throws IOException {
    resetComplexWritableList();

    VelocityContext context = buildContext(scenario);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("ComputeTestFuncTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * Generates the lines that construct {@link Writable} objects that are
   * used in the unittest.
   * @param className writable object's class name.
   * @return lines for constructing the {@link Writable} object.
   */
  public String generateReadWritableFromString(String className)
    throws IOException {
    VelocityContext context = new VelocityContext();
    context.put("class", className);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity
        .getTemplate("ReadWritableFromStringTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * @see #buildContext(GiraphVertexScenarioWrapper, String, String)
   * @param giraphScenarioWrapper {@link GiraphVertexScenarioWrapper} object.
   * @return {@link VelocityContext} to be used in generating the unittest file.
   */
  @SuppressWarnings("rawtypes")
  private VelocityContext buildContext(
    GiraphVertexScenarioWrapper giraphScenarioWrapper) {
    return buildContext(giraphScenarioWrapper, null, null);
  }

  /**
   * @param giraphScenarioWrapper {@link GiraphVertexScenarioWrapper} object.
   * @param testPackage name of the package for the unit test.
   * @param className name of the unit test class.
   * @return {@link VelocityContext} that will be used to generate the unit
   *         test.
   */
  @SuppressWarnings("rawtypes")
  private VelocityContext buildContext(
    GiraphVertexScenarioWrapper giraphScenarioWrapper, String testPackage,
    String className) {
    ComputeContextBuilder builder = new ComputeContextBuilder();
    VertexScenarioClassesWrapper vertexScenarioClassesWrapper
      = giraphScenarioWrapper
      .getVertexScenarioClassesWrapper();
    builder.addVertexScenarioClassesWrapper(vertexScenarioClassesWrapper);
    builder.addTestClassInfo(testPackage,
      vertexScenarioClassesWrapper.getClassUnderTest(), className);
    builder.addCommonMasterVertexContext(giraphScenarioWrapper
      .getContextWrapper().getCommonVertexMasterContextWrapper());
    builder.addVertexTypes(vertexScenarioClassesWrapper);
    builder.addVertexData(giraphScenarioWrapper.getContextWrapper());
    return builder.getContext();
  }

  /**
   * Wrapper to store information about the "context" of the compute method
   * being tested. Stores the vertex id type, vertex value types, message
   * types, etc. In addition stores the actual id, vertex values, etc..
   */
  protected class ComputeContextBuilder extends ContextBuilder {

    /**
     * @param vertexScenarioClassesWrapper
     *          {@link VertexScenarioClassesWrapper} object.
     */
    @SuppressWarnings("rawtypes")
    public void addVertexTypes(
      VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
      context.put("vertexIdType", vertexScenarioClassesWrapper
        .getVertexIdClass().getSimpleName());
      context.put("vertexValueType", vertexScenarioClassesWrapper
        .getVertexValueClass().getSimpleName());
      context.put("edgeValueType", vertexScenarioClassesWrapper
        .getEdgeValueClass().getSimpleName());
      context.put("inMsgType", vertexScenarioClassesWrapper
        .getIncomingMessageClass().getSimpleName());
      context.put("outMsgType", vertexScenarioClassesWrapper
        .getOutgoingMessageClass().getSimpleName());
    }

    /**
     * @param vertexContextWrapper {@link VertexContextWrapper} object to read
     *        vertex data from.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addVertexData(VertexContextWrapper vertexContextWrapper) {
      context.put("vertexId", vertexContextWrapper.getVertexIdWrapper());
      context.put("vertexValue",
        vertexContextWrapper.getVertexValueBeforeWrapper());
      context.put("vertexValueAfter",
        vertexContextWrapper.getVertexValueAfterWrapper());
      context.put("inMsgs", vertexContextWrapper.getIncomingMessageWrappers());
      context.put("neighbors", vertexContextWrapper.getNeighborWrappers());

      HashMap<OutgoingMessageWrapper, OutMsg> outMsgMap = new HashMap<>();
      for (OutgoingMessageWrapper msg :
        (Collection<OutgoingMessageWrapper>)
          vertexContextWrapper.getOutgoingMessageWrappers()) {
        if (outMsgMap.containsKey(msg)) {
          outMsgMap.get(msg).incrementTimes();
        } else {
          outMsgMap.put(msg, new OutMsg(msg));
        }
      }
      context.put("outMsgs", outMsgMap.values());
    }
  }

  /**
   * In-memory representation of the hadoop config values.
   */
  public static class Config {
    /**
     * Key of the configuration flag.
     */
    private final String key;
    /**
     * Value of the configuration flag.
     */
    private final Object value;

    /**
     * Constructor.
     * @param key key of the configuration flag.
     * @param value value of the configuration flag.
     */
    public Config(String key, Object value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    /**
     * @return value of the configuration flag.
     */
    public Object getValue() {
      if (value instanceof String) {
        return "\"" + value + '"';
      } else {
        return value;
      }
    }

    /**
     * @return returns type of the configuration's flag, e.g., Int, Float, ... .
     */
    public String getClassStr() {
      // TODO(brian):additional cases can be added up to the input
      if (value instanceof Integer) {
        return "Int";
      } else if (value instanceof Long) {
        return "Long";
      } else if (value instanceof Float) {
        return "Float";
      } else if (value instanceof Boolean) {
        return "Boolean";
      } else {
        return "";
      }
    }
  }

  /**
   * A wrapper around the {@link OutgoingMessageWrapper} that stores the
   * outgoing messages from a vertex and the number of times the message has
   * been sent.
   */
  @SuppressWarnings("rawtypes")
  public static class OutMsg {
    /**
     * {@link OutgoingMessageWrapper} object.
     */
    private final OutgoingMessageWrapper msg;
    /**
     * How many times the message has been sent.
     */
    private int times;

    /**
     * Constructor that initializes the outgoing message wrapper to msg and
     * the number of times the message has been sent.
     * @param msg outgoing message.
     */
    public OutMsg(OutgoingMessageWrapper msg) {
      this.msg = msg;
      this.times = 1;
    }

    public OutgoingMessageWrapper getMsg() {
      return msg;
    }

    public int getTimes() {
      return times;
    }

    /**
     * Increments the number of times this message has been sent by 1.
     */
    public void incrementTimes() {
      this.times++;
    }
  }
}
