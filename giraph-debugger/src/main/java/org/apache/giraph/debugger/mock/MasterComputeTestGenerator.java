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

import org.apache.giraph.debugger.utils.GiraphMasterScenarioWrapper;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

/**
 * A code generator to generate test cases to test {@link MasterCompute}
 *
 * author: Brian Truong Ba Quan
 */
public class MasterComputeTestGenerator extends TestGenerator {

  /**
   * Default constructor.
   */
  public MasterComputeTestGenerator() {
    super();
  }

  /**
   * Generates a unit test for the scenario stored in the
   * {@link GiraphMasterScenarioWrapper}.
   * @param scenario {@link GiraphMasterScenarioWrapper} object to generate a
   *        unit test from.
   * @param testPackage package of the unit test file.
   * @return unit test file's contents stored inside a string.
   */
  public String generateTest(GiraphMasterScenarioWrapper scenario,
    String testPackage) throws IOException,
    ClassNotFoundException {
    return generateTest(scenario, testPackage, null);
  }

  /**
   * Generates a unit test for the scenario stored in the
   * {@link GiraphMasterScenarioWrapper}.
   * @param scenario {@link GiraphMasterScenarioWrapper} object to generate a
   *        unit test from.
   * @param testPackage package of the unit test file.
   * @param className name of the unit test class.
   * @return unit test file's contents stored inside a string.
   */
  public String generateTest(GiraphMasterScenarioWrapper scenario,
    String testPackage, String className) throws IOException,
    ClassNotFoundException {
    VelocityContext context = buildContext(scenario, testPackage, className);

    try (StringWriter sw = new StringWriter()) {
      Template template = Velocity.getTemplate("MasterComputeTestTemplate.vm");
      template.merge(context, sw);
      return sw.toString();
    }
  }

  /**
   * Builds the {@link VelocityContext}, which stores data about the context of
   * the unit test, e.g. class types, that will be generated for the scenario
   * stored in the given {@link GiraphMasterScenarioWrapper}.
   *
   * @param scenario
   *          {@link GiraphMasterScenarioWrapper} object to generate a unit test
   *          from.
   * @param testPackage
   *          package of the unit test file.
   * @param className
   *          name of the unit test class.
   * @return {@link VelocityContext} object storing data about the generated
   *         unit test.
   */
  private VelocityContext buildContext(
    GiraphMasterScenarioWrapper scenario, String testPackage,
    String className) throws ClassNotFoundException {
    ContextBuilder builder = new ContextBuilder();

    Class<?> classUnderTest = Class.forName(scenario
      .getMasterClassUnderTest());
    builder.addTestClassInfo(testPackage, classUnderTest, className);
    builder.addCommonMasterVertexContext(scenario
      .getCommonVertexMasterContextWrapper());

    return builder.getContext();
  }
}
