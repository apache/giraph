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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.debugger.mock.ComputationComputeTestGenerator.Config;
import org.apache.giraph.debugger.utils.AggregatedValueWrapper;
import org.apache.giraph.debugger.utils.CommonVertexMasterContextWrapper;
import org.apache.giraph.debugger.utils.GiraphVertexScenarioWrapper.VertexScenarioClassesWrapper;
import org.apache.velocity.VelocityContext;

/**
 * Base clas for {@link ComputationComputeTestGenerator} and
 * {@link MasterComputeTestGenerator}.
 */
public abstract class TestGenerator extends VelocityBasedGenerator {

  /**
   * A set of complex writable classes, i.e., (typically) user-defined
   * writables more complex that the regular {@link IntWritable},
   * {@link LongWritable}, etc.
   */
  @SuppressWarnings("rawtypes")
  private final Set<Class> complexWritables = new HashSet<>();

  @SuppressWarnings("rawtypes")
  public Set<Class> getComplexWritableList() {
    return complexWritables;
  }

  /**
   * Clears {@link #complexWritables}.
   */
  protected void resetComplexWritableList() {
    this.complexWritables.clear();
  }

  /**
   * A wrapper class to populate a {@link VelocityContext} object.
   */
  protected class ContextBuilder {

    /**
     * {@link VelocityContext} object being wrapped.
     */
    protected VelocityContext context;

    /**
     * Default constructor.
     */
    public ContextBuilder() {
      context = new VelocityContext();
      addHelper();
      addWritableReadFromString();
    }

    public VelocityContext getContext() {
      return context;
    }

    /**
     * Adds a {@link FormatHelper} to the context.
     */
    private void addHelper() {
      context.put("helper", new FormatHelper(complexWritables));
    }

    /**
     * Adds the complex writables to the context.
     */
    private void addWritableReadFromString() {
      context.put("complexWritables", complexWritables);
    }

    /**
     * Adds the given package name to the context.
     * @param testPackage name of the package for the unit test file.
     */
    public void addPackage(String testPackage) {
      context.put("package", testPackage);
    }

    /**
     * Adds the type of the class that is being tested to the context.
     * @param classUnderTest the class that is being tested.
     */
    @SuppressWarnings("rawtypes")
    public void addClassUnderTest(Class classUnderTest) {
      context.put("classUnderTestFullName", classUnderTest.getName());
      context.put("classUnderTestName", classUnderTest.getSimpleName());
      context.put("classUnderTestPackage", classUnderTest.getPackage()
        .getName());
    }

    /**
     * Adds the string name of the class that is being tested to the context.
     * @param className name of the class being tested.
     */
    public void addClassName(String className) {
      if (className == null) {
        context.put("className", context.get("classUnderTestName") + "Test");
      } else {
        context.put("className", className);
      }
    }

    /**
     * Adds the package, type, and name of the class that is being tested to
     * the context.
     * @param testPackage name of the package for the unit test file.
     * @param classUnderTest the class that is being tested.
     * @param className name of the class being tested.
     */
    @SuppressWarnings("rawtypes")
    public void addTestClassInfo(String testPackage, Class classUnderTest,
      String className) {
      addPackage(testPackage);
      addClassUnderTest(classUnderTest);
      addClassName(className);
    }

    /**
     * Adds data stored in the given vertex scenario wrapper to the context.
     * @param vertexScenarioClassesWrapper
     *          {@link VertexScenarioClassesWrapper} object.
     */
    @SuppressWarnings("rawtypes")
    public void addVertexScenarioClassesWrapper(
      VertexScenarioClassesWrapper vertexScenarioClassesWrapper) {
      HashSet<Class> usedTypes = new LinkedHashSet<>(6);
      usedTypes.add(vertexScenarioClassesWrapper.getClassUnderTest());
      usedTypes.add(vertexScenarioClassesWrapper.getVertexIdClass());
      usedTypes.add(vertexScenarioClassesWrapper.getVertexValueClass());
      usedTypes.add(vertexScenarioClassesWrapper.getEdgeValueClass());
      usedTypes.add(vertexScenarioClassesWrapper.getIncomingMessageClass());
      usedTypes.add(vertexScenarioClassesWrapper.getOutgoingMessageClass());
      context.put("usedTypes", usedTypes);
    }

    /**
     * Adds data stored in the given master scenario wrapper to the context.
     * @param commonVertexMasterContextWrapper
     *          {@link CommonVertexMasterContextWrapper} object.
     */
    @SuppressWarnings("rawtypes")
    public void addCommonMasterVertexContext(
      CommonVertexMasterContextWrapper commonVertexMasterContextWrapper) {
      context.put("superstepNo",
        commonVertexMasterContextWrapper.getSuperstepNoWrapper());
      context.put("nVertices",
        commonVertexMasterContextWrapper.getTotalNumVerticesWrapper());
      context.put("nEdges",
        commonVertexMasterContextWrapper.getTotalNumEdgesWrapper());

      context.put("aggregators",
        commonVertexMasterContextWrapper.getPreviousAggregatedValues());
      Set<Class> usedTypes = new LinkedHashSet<>();
      Collection<AggregatedValueWrapper> aggregatedValues =
        commonVertexMasterContextWrapper.getPreviousAggregatedValues();
      for (AggregatedValueWrapper aggregatedValueWrapper : aggregatedValues) {
        usedTypes.add(aggregatedValueWrapper.getValue().getClass());
      }
      context.put("usedTypesByAggregators", usedTypes);

      List<Config> configs = new ArrayList<>();
      if (commonVertexMasterContextWrapper.getConfig() != null) {
        for (Map.Entry<String, String> entry : (Iterable<Map.Entry<
          String, String>>) commonVertexMasterContextWrapper.getConfig()) {
          configs.add(new Config(entry.getKey(), entry.getValue()));
        }
      }
      context.put("configs", configs);
    }
  }
}
