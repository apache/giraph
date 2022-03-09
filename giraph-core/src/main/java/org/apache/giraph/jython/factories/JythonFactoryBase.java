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

import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.factories.ValueFactory;
import org.apache.giraph.graph.GraphType;
import org.apache.giraph.jython.JythonOptions;
import org.apache.giraph.jython.JythonUtils;
import org.apache.giraph.jython.wrappers.JythonWritableWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.python.core.PyObject;

/**
 * Base class for Jython factories. These factories are used to create user's
 * graph value types (IVEMM).
 *
 * @param <W> writable type
 */
public abstract class JythonFactoryBase<W extends Writable>
    implements ValueFactory<W>, GiraphConfigurationSettable {
  /** Logger */
  private static final Logger LOG = Logger.getLogger(JythonFactoryBase.class);

  /** Name of class in Jython implementing Vertex ID */
  private String jythonClassName;
  /** Whether the Jython type needs a wrapper */
  private boolean useWrapper;

  /**
   * Get the options associated with this graph type
   *
   * @return options
   */
  public abstract JythonOptions.JythonGraphTypeOptions getOptions();

  /**
   * Get the graph type this factory is meant for
   *
   * @return GraphType
   */
  public GraphType getGraphType() {
    return getOptions().getGraphType();
  }

  /**
   * The {@link org.apache.hadoop.conf.Configuration} option for setting the
   * Jython class name in this factory implementation.
   *
   * @return {@link org.apache.giraph.conf.StrConfOption}
   */
  public StrConfOption jythonClassNameOption() {
    return getOptions().getJythonClassNameOption();
  }

  /**
   * The interface class for the value type.
   * For Vertex ID this is {@link org.apache.hadoop.io.WritableComparable}, for
   * others this is {@link org.apache.hadoop.io.Writable}
   *
   * @return interface class for the value type
   */
  public Class<? extends Writable> writableValueClass() {
    return getGraphType().interfaceClass();
  }

  @Override
  public void setConf(
      ImmutableClassesGiraphConfiguration conf) {
    jythonClassName = jythonClassNameOption().get(conf);
    useWrapper = conf.getValueNeedsWrappers().get(getGraphType());
  }

  /**
   * Instantiate a new value Jython object.
   *
   * @return new value object
   */
  public Writable newJythonClassInstance() {
    if (useWrapper) {
      PyObject jythonObj = JythonUtils.newInstance(jythonClassName);
      JythonWritableWrapper wrapper = new JythonWritableWrapper(jythonObj);
      return wrapper;
    } else {
      return JythonUtils.newInstance(jythonClassName, writableValueClass());
    }
  }

  /**
   * Use this factory in the {@link org.apache.hadoop.conf.Configuration}
   *
   * @param conf {@link org.apache.hadoop.conf.Configuration}
   * @param jythonClassName Name of Jython class implementing value type
   */
  public void useThisFactory(Configuration conf, String jythonClassName) {
    if (LOG.isInfoEnabled()) {
      LOG.info("useThisFactory: Setting up Jython factory for " +
          getGraphType() + " reading " + " using Jython type " +
          jythonClassName);
    }

    ClassConfOption factoryOption = getGraphType().factoryClassOption();
    factoryOption.set(conf, getClass());

    jythonClassNameOption().set(conf, jythonClassName);
  }
}
