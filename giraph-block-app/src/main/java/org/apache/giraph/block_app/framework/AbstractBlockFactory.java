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
package org.apache.giraph.block_app.framework;

import java.util.List;

import org.apache.giraph.comm.messages.MessageEncodeAndStoreType;
import org.apache.giraph.conf.BulkConfigurator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.edge.IdAndNullArrayEdges;
import org.apache.giraph.edge.IdAndValueArrayEdges;
import org.apache.giraph.edge.LongDiffNullArrayEdges;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default block factory abstract class, providing default methods that need
 * to be/can be overridden for specifying required/most common parameters,
 * to simplify setting properties.
 *
 * @param <S> Execution stage type
 */
public abstract class AbstractBlockFactory<S> implements BlockFactory<S> {
  /**
   * Comma separated list of BulkConfigurators, that are going to be called
   * to simplify specifying of large number of properties.
   */
  public static final StrConfOption CONFIGURATORS = new StrConfOption(
      "digraph.block_factory_configurators", null, "");

  @Override
  public List<String> getGcJavaOpts(Configuration conf) {
    return null;
  }

  @Override
  public final void initConfig(GiraphConfiguration conf) {
    initConfigurators(conf);
    GiraphConstants.VERTEX_ID_CLASS.setIfUnset(conf, getVertexIDClass(conf));
    GiraphConstants.VERTEX_VALUE_CLASS.setIfUnset(
        conf, getVertexValueClass(conf));
    GiraphConstants.EDGE_VALUE_CLASS.setIfUnset(conf, getEdgeValueClass(conf));
    GiraphConstants.RESOLVER_CREATE_VERTEX_ON_MSGS.setIfUnset(
        conf, shouldCreateVertexOnMsgs(conf));
    if (shouldSendOneMessageToAll(conf)) {
      GiraphConstants.MESSAGE_ENCODE_AND_STORE_TYPE.setIfUnset(
          conf, MessageEncodeAndStoreType.EXTRACT_BYTEARRAY_PER_PARTITION);
    }

    BlockUtils.BLOCK_WORKER_CONTEXT_VALUE_CLASS.setIfUnset(
        conf, getWorkerContextValueClass(conf));

    // optimize edge structure, if available and not set already
    if (!GiraphConstants.VERTEX_EDGES_CLASS.contains(conf)) {
      @SuppressWarnings("rawtypes")
      Class<? extends WritableComparable> vertexIDClass =
          GiraphConstants.VERTEX_ID_CLASS.get(conf);
      Class<? extends Writable> edgeValueClass =
          GiraphConstants.EDGE_VALUE_CLASS.get(conf);


      @SuppressWarnings("rawtypes")
      PrimitiveIdTypeOps<? extends WritableComparable> idTypeOps =
          TypeOpsUtils.getPrimitiveIdTypeOpsOrNull(vertexIDClass);
      if (edgeValueClass.equals(NullWritable.class)) {
        if (vertexIDClass.equals(LongWritable.class)) {
          GiraphConstants.VERTEX_EDGES_CLASS.set(
              conf, LongDiffNullArrayEdges.class);
        } else if (idTypeOps != null) {
          GiraphConstants.VERTEX_EDGES_CLASS.set(
              conf, IdAndNullArrayEdges.class);
        }
      } else {
        TypeOps<?> edgeValueTypeOps =
            TypeOpsUtils.getTypeOpsOrNull(edgeValueClass);
        if (edgeValueTypeOps != null && idTypeOps != null) {
          GiraphConstants.VERTEX_EDGES_CLASS.set(
              conf, IdAndValueArrayEdges.class);
        }
      }
    }

    additionalInitConfig(conf);
  }

  @Override
  public void registerOutputs(GiraphConfiguration conf) {
  }

  private void initConfigurators(GiraphConfiguration conf) {
    String configurators = CONFIGURATORS.get(conf);
    if (configurators != null) {
      String[] split = configurators.split(",");
      for (String configurator : split) {
        runConfigurator(conf, configurator);
      }
    }
  }

  private void runConfigurator(GiraphConfiguration conf, String configurator) {
    String[] packages = getConvenienceConfiguratorPackages();
    String[] prefixes = new String[packages.length + 1];
    prefixes[0] = "";
    for (int i = 0; i < packages.length; i++) {
      prefixes[i + 1] = packages[i] + ".";
    }

    for (String prefix : prefixes) {
      try {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Class<BulkConfigurator> confClass =
            (Class) Class.forName(prefix + configurator);
        BulkConfigurator c = ReflectionUtils.newInstance(confClass);
        c.configure(conf);
        return;
      // CHECKSTYLE: stop EmptyBlock
      // ignore ClassNotFoundException, and continue the loop
      } catch (ClassNotFoundException e) {
      }
      // CHECKSTYLE: resume EmptyBlock
    }
    throw new IllegalStateException(
        "Configurator " + configurator + " not found");
  }

  /**
   * Additional configuration initialization, other then overriding
   * class specification.
   */
  protected void additionalInitConfig(GiraphConfiguration conf) {
  }

  /**
   * Concrete vertex id class application will use.
   */
  @SuppressWarnings("rawtypes")
  protected abstract Class<? extends WritableComparable> getVertexIDClass(
      GiraphConfiguration conf);

  /**
   * Concrete vertex value class application will use.
   */
  protected abstract Class<? extends Writable> getVertexValueClass(
      GiraphConfiguration conf);

  /**
   * Concrete edge value class application will use.
   */
  protected abstract Class<? extends Writable> getEdgeValueClass(
      GiraphConfiguration conf);

  /**
   * Concrete worker context value class application will use, if overridden.
   */
  protected Class<?> getWorkerContextValueClass(GiraphConfiguration conf) {
    return Object.class;
  }

  /**
   * Override if vertices shouldn't be created by default, if message is sent
   * to a vertex that doesn't exist.
   */
  protected boolean shouldCreateVertexOnMsgs(GiraphConfiguration conf) {
    return true;
  }

  // TODO - see if it should be deprecated
  protected boolean shouldSendOneMessageToAll(GiraphConfiguration conf) {
    return false;
  }

  /**
   * Provide list of strings representing packages where configurators will
   * be searched for, allowing that full path is not required for
   * CONFIGURATORS option.
   */
  protected String[] getConvenienceConfiguratorPackages() {
    return new String[] { };
  }
}
