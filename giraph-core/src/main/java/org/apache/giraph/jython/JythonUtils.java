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
package org.apache.giraph.jython;

import org.apache.giraph.graph.Language;
import org.apache.hadoop.conf.Configuration;
import org.python.util.PythonInterpreter;

import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_FACTORY_CLASS;
import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_LANGUAGE;
import static org.apache.giraph.jython.JythonComputationFactory.JYTHON_COMPUTATION_CLASS_NAME;

/**
 * Helpers for running jobs with Jython.
 */
public class JythonUtils {
  /**
   * The Jython interpreter. Cached here for fast access. We use a singleton
   * for this so that we can parse all of the Jython scripts once at startup
   * and then have their data loaded for the rest of the job.
   */
  private static final PythonInterpreter INTERPRETER =
      new PythonInterpreter();

  /** Don't construct */
  private JythonUtils() { }

  /**
   * Get Jython interpreter
   *
   * @return interpreter
   */
  public static PythonInterpreter getInterpreter() {
    return INTERPRETER;
  }

  /**
   * Sets up the Configuration for using Jython
   *
   * @param conf Configuration to se
   * @param klassName Class name of Jython Computation
   */
  public static void init(Configuration conf, String klassName) {
    COMPUTATION_LANGUAGE.set(conf, Language.JYTHON);
    COMPUTATION_FACTORY_CLASS.set(conf, JythonComputationFactory.class);
    JYTHON_COMPUTATION_CLASS_NAME.set(conf, klassName);
  }
}
