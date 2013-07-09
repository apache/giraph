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

import org.apache.giraph.conf.EnumConfOption;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.factories.ComputationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.giraph.utils.DistributedCacheUtils.getLocalCacheFile;

/**
 * Factory for creating Jython Computation from python scripts
 */
public class JythonComputationFactory implements ComputationFactory {
  /** Type of script path */
  public static final EnumConfOption<DeployType> JYTHON_DEPLOY_TYPE =
      EnumConfOption.create("giraph.jython.deploy.type",
          DeployType.class, DeployType.DISTRIBUTED_CACHE,
          "Type of script path");
  /** Path to Jython script */
  public static final StrConfOption JYTHON_SCRIPT_PATH =
      new StrConfOption("giraph.jython.path", "_script_not_set_",
          "Path to Jython script");
  /** Name of Computation class in Jython script */
  public static final StrConfOption JYTHON_COMPUTATION_CLASS =
      new StrConfOption("giraph.jython.class", "_computation_class_not_set_",
          "Name of Computation class in Jython script");

  /** Logger */
  private static final Logger LOG = Logger.getLogger(JythonUtils.class);

  @Override
  public void initialize(ImmutableClassesGiraphConfiguration conf) {
    String scriptPath = JYTHON_SCRIPT_PATH.get(conf);
    InputStream pythonStream = getPythonScriptStream(conf, scriptPath);
    try {
      PythonInterpreter interpreter = new PythonInterpreter();
      if (LOG.isInfoEnabled()) {
        LOG.info("initComputation: Jython loading script " + scriptPath);
      }
      interpreter.execfile(pythonStream);

      String className = computationName(conf);
      PyObject pyComputationModule = interpreter.get(className);

      JythonUtils.setPythonComputationModule(pyComputationModule);
    } finally {
      Closeables.closeQuietly(pythonStream);
    }
  }

  /**
   * Get an {@link InputStream} for the jython script.
   *
   * @param conf Configuration
   * @param path script path
   * @return {@link InputStream} for reading script
   */
  private InputStream getPythonScriptStream(Configuration conf,
      String path) {
    InputStream stream = null;
    DeployType deployType = JYTHON_DEPLOY_TYPE.get(conf);
    switch (deployType) {
    case RESOURCE:
      if (LOG.isInfoEnabled()) {
        LOG.info("getPythonScriptStream: Reading Jython Computation " +
            "from resource at " + path);
      }
      stream = getClass().getResourceAsStream(path);
      if (stream == null) {
        throw new IllegalStateException("getPythonScriptStream: Failed to " +
            "open Jython script from resource at " + path);
      }
      break;
    case DISTRIBUTED_CACHE:
      if (LOG.isInfoEnabled()) {
        LOG.info("getPythonScriptStream: Reading Jython Computation " +
            "from DistributedCache at " + path);
      }
      Optional<Path> localPath = getLocalCacheFile(conf, path);
      if (!localPath.isPresent()) {
        throw new IllegalStateException("getPythonScriptStream: Failed to " +
            "find Jython script in local DistributedCache matching " + path);
      }
      String pathStr = localPath.get().toString();
      try {
        stream = new BufferedInputStream(new FileInputStream(pathStr));
      } catch (IOException e) {
        throw new IllegalStateException("getPythonScriptStream: Failed open " +
            "Jython script from DistributedCache at " + localPath);
      }
      break;
    default:
      throw new IllegalArgumentException("getPythonScriptStream: Unknown " +
          "Jython script deployment type: " + deployType);
    }
    return stream;
  }

  @Override
  public Computation createComputation(
      ImmutableClassesGiraphConfiguration conf) {
    PyObject pyComputationModule = JythonUtils.getPythonComputationModule();
    Preconditions.checkNotNull(pyComputationModule);

    PyObject pyComputationObj = pyComputationModule.__call__();
    Object computationObj = pyComputationObj.__tojava__(Computation.class);
    if (!(computationObj instanceof Computation)) {
      throw new IllegalStateException("getComputation: Jython object " +
          computationName(conf) + " is not a Computation");
    }

    conf.configureIfPossible(computationObj);
    return (Computation) computationObj;
  }

  @Override
  public void checkConfiguration(ImmutableClassesGiraphConfiguration conf) {
    if (JYTHON_SCRIPT_PATH.isDefaultValue(conf)) {
      throw new IllegalStateException("checkConfiguration: " +
          JYTHON_SCRIPT_PATH.getKey() + " not set in configuration");
    }
    if (JYTHON_COMPUTATION_CLASS.isDefaultValue(conf)) {
      throw new IllegalStateException("checkConfiguration: " +
          JYTHON_COMPUTATION_CLASS.getKey() + " not set in configuration");
    }
  }

  @Override
  public String computationName(GiraphConfiguration conf) {
    return JYTHON_COMPUTATION_CLASS.get(conf);
  }
}
