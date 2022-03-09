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

package org.apache.giraph.worker;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.mapping.MappingStore;
import org.apache.giraph.mapping.MappingStoreOps;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Stores LocalData for each worker
 *
 * @param <I> vertexId type
 * @param <V> vertex value type
 * @param <E> edge value type
 * @param <B> mappingTarget type
 */
@SuppressWarnings("unchecked")
public class LocalData<I extends WritableComparable, V extends Writable,
  E extends Writable, B extends Writable>
  extends DefaultImmutableClassesGiraphConfigurable<I, V, E> {
  /** Logger Instance */
  private static final Logger LOG = Logger.getLogger(LocalData.class);
  /** MappingStore from vertexId - target */
  private MappingStore<I, B> mappingStore;
  /** Do operations using mapping store */
  private MappingStoreOps<I, B> mappingStoreOps;
  /**
   * Constructor
   *
   * Set configuration, create &amp; initialize mapping store
   * @param conf giraph configuration
   */
  public LocalData(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    // set configuration
    setConf(conf);
    // check if user set the mapping store => create & initialize it
    mappingStore = (MappingStore<I, B>) getConf().createMappingStore();
    if (mappingStore != null) {
      mappingStore.initialize();
    }
    mappingStoreOps = (MappingStoreOps<I, B>) getConf().createMappingStoreOps();
    if (mappingStoreOps != null) {
      mappingStoreOps.initialize(mappingStore);
    }
  }

  public MappingStore<I, B> getMappingStore() {
    return mappingStore;
  }

  public MappingStoreOps<I, B> getMappingStoreOps() {
    return mappingStoreOps;
  }

  /**
   * Remove mapping store from localData
   * if mapping data is already embedded into vertexIds
   */
  public void removeMappingStoreIfPossible() {
    if (mappingStoreOps != null && mappingStoreOps.hasEmbedding()) {
      mappingStore = null;
    }
  }

  /**
   * Prints Stats of individual data it stores
   */
  public void printStats() {
    if (LOG.isInfoEnabled()) {
      LOG.info("MappingStore has : " + mappingStore.getStats() + " entries");
    }
  }
}
