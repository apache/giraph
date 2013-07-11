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
package org.apache.giraph.factories;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import static org.apache.giraph.conf.GiraphConstants.EDGE_VALUE_FACTORY_CLASS;
import static org.apache.giraph.conf.GiraphConstants.INCOMING_MESSAGE_VALUE_FACTORY_CLASS;
import static org.apache.giraph.conf.GiraphConstants.OUTGOING_MESSAGE_VALUE_FACTORY_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_ID_FACTORY_CLASS;
import static org.apache.giraph.conf.GiraphConstants.VERTEX_VALUE_FACTORY_CLASS;

/**
 * Holder for factories to create user types.
 *
 * Note that we don't store the {@link MessageValueFactory} here because they
 * reference types which may change at a given superstep. Instead we create them
 * as necessary so that they get the latest information.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class ValueFactories<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Vertex ID factory. */
  private final VertexIdFactory<I> vertexIdFactory;
  /** Vertex value factory. */
  private final VertexValueFactory<V> vertexValueFactory;
  /** Edge value factory. */
  private final EdgeValueFactory<E> edgeValueFactory;
  // Note that for messages we store the class not the factory itself, because
  // the factory instance may change per-superstep if the graph types change.
  /** Incoming message value factory class */
  private final Class<? extends MessageValueFactory> inMsgFactoryClass;
  /** Outgoing message value factory class */
  private final Class<? extends MessageValueFactory> outMsgFactoryClass;

  /**
   * Constructor reading from Configuration
   *
   * @param conf Configuration to read from
   */
  public ValueFactories(Configuration conf) {
    vertexIdFactory = VERTEX_ID_FACTORY_CLASS.newInstance(conf);
    vertexValueFactory = VERTEX_VALUE_FACTORY_CLASS.newInstance(conf);
    edgeValueFactory = EDGE_VALUE_FACTORY_CLASS.newInstance(conf);
    inMsgFactoryClass = INCOMING_MESSAGE_VALUE_FACTORY_CLASS.get(conf);
    outMsgFactoryClass = OUTGOING_MESSAGE_VALUE_FACTORY_CLASS.get(conf);
  }

  /**
   * Initialize all of the factories.
   *
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public void initializeIVE(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    vertexIdFactory.initialize(conf);
    vertexValueFactory.initialize(conf);
    edgeValueFactory.initialize(conf);
  }

  public EdgeValueFactory<E> getEdgeValueFactory() {
    return edgeValueFactory;
  }

  public VertexIdFactory<I> getVertexIdFactory() {
    return vertexIdFactory;
  }

  public VertexValueFactory<V> getVertexValueFactory() {
    return vertexValueFactory;
  }

  public Class<? extends MessageValueFactory> getInMsgFactoryClass() {
    return inMsgFactoryClass;
  }

  public Class<? extends MessageValueFactory> getOutMsgFactoryClass() {
    return outMsgFactoryClass;
  }
}
