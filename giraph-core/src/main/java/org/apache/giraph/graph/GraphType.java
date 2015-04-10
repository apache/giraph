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
package org.apache.giraph.graph;

import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.ValueFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * Enumeration for a user graph type (IVEMM)
 */
public enum GraphType {
  /** Vertex ID */
  VERTEX_ID {
    @Override
    public ClassConfOption<? extends Writable> writableConfOption() {
      return GiraphConstants.VERTEX_ID_CLASS;
    }
    @Override
    public ClassConfOption<? extends ValueFactory> factoryClassOption() {
      return GiraphConstants.VERTEX_ID_FACTORY_CLASS;
    }
    @Override
    public <T extends Writable> Class<T> get(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getVertexIdClass();
    }
    @Override
    public <T extends Writable> ValueFactory<T> factory(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getVertexIdFactory();
    }
  },
  /** Vertex Value */
  VERTEX_VALUE {
    @Override public ClassConfOption<? extends Writable> writableConfOption() {
      return GiraphConstants.VERTEX_VALUE_CLASS;
    }
    @Override
    public ClassConfOption<? extends ValueFactory> factoryClassOption() {
      return GiraphConstants.VERTEX_VALUE_FACTORY_CLASS;
    }
    @Override
    public <T extends Writable> Class<T> get(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getVertexValueClass();
    }
    @Override
    public <T extends Writable> ValueFactory<T> factory(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getVertexValueFactory();
    }
  },
  /** Edge Value */
  EDGE_VALUE {
    @Override
    public ClassConfOption<? extends Writable> writableConfOption() {
      return GiraphConstants.EDGE_VALUE_CLASS;
    }
    @Override
    public ClassConfOption<? extends ValueFactory> factoryClassOption() {
      return GiraphConstants.EDGE_VALUE_FACTORY_CLASS;
    }
    @Override
    public <T extends Writable> Class<T> get(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getEdgeValueClass();
    }
    @Override
    public <T extends Writable> ValueFactory<T> factory(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getEdgeValueFactory();
    }
  },
  /** Outgoing message value */
  OUTGOING_MESSAGE_VALUE {
    @Override
    public ClassConfOption<? extends Writable> writableConfOption() {
      return GiraphConstants.OUTGOING_MESSAGE_VALUE_CLASS;
    }
    @Override
    public ClassConfOption<? extends ValueFactory> factoryClassOption() {
      return GiraphConstants.OUTGOING_MESSAGE_VALUE_FACTORY_CLASS;
    }
    @Override
    public <T extends Writable> Class<T> get(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.getOutgoingMessageValueClass();
    }
    @Override
    public <T extends Writable> ValueFactory<T> factory(
        ImmutableClassesGiraphConfiguration conf) {
      return conf.createOutgoingMessageValueFactory();
    }
  };

  @Override
  public String toString() {
    return name().toLowerCase();
  }

  /**
   * Name of graph type with dots, like vertex.id or vertex.value
   *
   * @return name dot string
   */
  public String dotString() {
    return toString().replaceAll("_", ".");
  }

  /**
   * Name of graph type with spaces, like "vertex id" or "vertex value"
   *
   * @return name space string
   */
  public String spaceString() {
    return toString().replaceAll("_", " ");
  }

  /**
   * Get class set by user for this option
   *
   * @param <T> writable type
   * @param conf Configuration
   * @return Class for graph type
   */
  public abstract <T extends Writable> Class<T> get(
      ImmutableClassesGiraphConfiguration conf);

  /**
   * Get factory for creating this graph type
   *
   * @param <T> writable type
   * @param conf Configuration
   * @return Factory for this graph type
   */
  public abstract <T extends Writable> ValueFactory<T> factory(
      ImmutableClassesGiraphConfiguration conf);

  /**
   * Get the option for the factory that creates this graph type
   *
   * @param <T> writable type
   * @return Factory class option
   */
  public abstract <T extends ValueFactory> ClassConfOption<T>
  factoryClassOption();

  /**
   * Get the class option for this graph type
   *
   * @param <T> writable type
   * @return ClassConfOption option
   */
  public abstract <T extends Writable> ClassConfOption<T> writableConfOption();

  /**
   * Get interface class (Writable or WritableComparable) for this graph type
   *
   * @param <T> writable type
   * @return interface Writable class
   */
  public <T extends Writable> Class<T> interfaceClass() {
    return this.<T>writableConfOption().getInterfaceClass();
  }

  /**
   * Get class set by user for this option
   *
   * @param <T> writable type
   * @param conf Configuration
   * @return Class for graph type
   */
  public <T extends Writable> Class<? extends T> get(Configuration conf) {
    if (conf instanceof ImmutableClassesGiraphConfiguration) {
      ImmutableClassesGiraphConfiguration icgc =
          (ImmutableClassesGiraphConfiguration) conf;
      return get(icgc);
    }
    return this.<T>writableConfOption().get(conf);
  }

  /**
   * Create a new instance of this value type from configuration
   *
   * @param <T> writable type
   * @param conf {@link ImmutableClassesGiraphConfiguration}
   * @return new instance of this graph value type
   */
  public <T extends Writable> T newInstance(
      ImmutableClassesGiraphConfiguration conf) {
    return (T) factory(conf).newInstance();
  }
}
