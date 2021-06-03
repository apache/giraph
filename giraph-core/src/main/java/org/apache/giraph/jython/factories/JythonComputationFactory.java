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
package org.apache.giraph.jython.factories;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.jython.JythonComputation;
import org.apache.giraph.jython.JythonGiraphComputation;
import org.apache.giraph.jython.JythonOptions;
import org.apache.giraph.jython.JythonUtils;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.log4j.Logger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.giraph.scripting.ScriptLoader.SCRIPTS_TO_LOAD;

/**
 * Factory for creating Jython Computation from python scripts
 */
public class JythonComputationFactory implements ComputationFactory {
  /** The Jython compute function, cached here for fast access */
  private static volatile PyObject JYTHON_COMPUTATION_MODULE;

  /** Logger */
  private static final Logger LOG = Logger.getLogger(JythonUtils.class);

  /**
   * Set static python computation module stored
   *
   * @param mod python computation module
   */
  private static void setPythonComputationModule(PyObject mod) {
    JYTHON_COMPUTATION_MODULE = mod;
  }

  /**
   * Get python computation module stored
   *
   * @return python computation module
   */
  private static PyObject getPythonComputationModule() {
    return JYTHON_COMPUTATION_MODULE;
  }

  @Override
  public void initialize(ImmutableClassesGiraphConfiguration conf) {
    PythonInterpreter interpreter = JythonUtils.getInterpreter();
    String className = computationName(conf);
    PyObject pyComputationModule = interpreter.get(className);
    checkNotNull(pyComputationModule,
        "Could not find Jython Computation class " + className +
        " in loaded scripts: " + ScriptLoader.getLoadedScripts());
    setPythonComputationModule(pyComputationModule);
  }

  @Override
  public Computation createComputation(
      ImmutableClassesGiraphConfiguration conf) {
    checkNotNull(JYTHON_COMPUTATION_MODULE,
        "Jython Computation class not set in loaded scripts: " +
        ScriptLoader.getLoadedScripts());

    PyObject pyComputationObj = JYTHON_COMPUTATION_MODULE.__call__();
    Object computeObj = pyComputationObj.__tojava__(JythonComputation.class);
    if (!(computeObj instanceof JythonComputation)) {
      throw new IllegalStateException("getComputation: Jython object " +
          computationName(conf) + " is not a JythonComputation");
    }
    JythonComputation jythonCompute = (JythonComputation) computeObj;
    conf.configureIfPossible(jythonCompute);

    JythonGiraphComputation giraphCompute =
        new JythonGiraphComputation(jythonCompute);
    giraphCompute.setConf(conf);

    jythonCompute.setGiraphCompute(giraphCompute);

    return giraphCompute;
  }

  @Override
  public void checkConfiguration(ImmutableClassesGiraphConfiguration conf) {
    if (SCRIPTS_TO_LOAD.isDefaultValue(conf)) {
      throw new IllegalStateException("checkConfiguration: " +
          SCRIPTS_TO_LOAD.getKey() + " not set in configuration");
    }
    if (!JythonOptions.JYTHON_COMPUTATION_CLASS_NAME.contains(conf)) {
      throw new IllegalStateException("checkConfiguration: " +
          JythonOptions.JYTHON_COMPUTATION_CLASS_NAME.getKey() +
          " not set in configuration");
    }
  }

  @Override
  public String computationName(GiraphConfiguration conf) {
    return JythonOptions.JYTHON_COMPUTATION_CLASS_NAME.get(conf);
  }
}
