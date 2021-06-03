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

package org.apache.giraph.ooc.persistence;

import java.util.ArrayList;
import java.util.List;

/**
 * Index chain used in out-of-core data accessor object (DAO) to access
 * serialized data.
 */
public class DataIndex {
  /** Chain of data indices */
  private final List<DataIndexEntry> indexList = new ArrayList<>(5);

  /**
   * Add an index to the index chain
   *
   * @param entry the entry to add to the chain
   * @return the index chain itself
   */
  public DataIndex addIndex(DataIndexEntry entry) {
    indexList.add(entry);
    return this;
  }

  /**
   * Remove/Pop the last index in the index chain
   *
   * @return the index chain itself
   */
  public DataIndex removeLastIndex() {
    indexList.remove(indexList.size() - 1);
    return this;
  }

  /**
   * Create a copy of the existing DataIndex
   *
   * @return a copy of the existing index chain
   */
  public DataIndex copy() {
    DataIndex index = new DataIndex();
    for (DataIndexEntry entry : indexList) {
      index.indexList.add(entry);
    }
    return index;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DataIndex)) {
      return false;
    }
    DataIndex dataIndex = (DataIndex) obj;
    return indexList.equals(dataIndex.indexList);
  }

  @Override
  public int hashCode() {
    return indexList.hashCode();
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (DataIndexEntry entry : indexList) {
      sb.append(entry);
    }
    return sb.toString();
  }

  /** Interface to unify different types of entries used as index chain */
  public interface DataIndexEntry { }

  /**
   * Different static types of index chain entry
   */
  public enum TypeIndexEntry implements DataIndexEntry {
    /** The whole partition */
    PARTITION("_partition"),
    /** Partition vertices */
    PARTITION_VERTICES("_vertices"),
    /** Partition edges */
    PARTITION_EDGES("_edges"),
    /** Partition messages */
    MESSAGE("_messages"),
    /** Edges stored in edge store for a partition */
    EDGE_STORE("_edge_store"),
    /** Raw data buffer (refer to DiskBackedDataStore) */
    BUFFER("_buffer");

    /** String realization of entry type */
    private final String name;

    /**
     * Constructor
     *
     * @param name name of the type
     */
    TypeIndexEntry(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * Class representing any index chain that depends on something with id.
   * Generally this is used for identifying indices in two types:
   *  - Index entry based on superstep id ('S' and the superstep number)
   *  - Index entry based on partition id ('P' and the partition id)
   */
  public static final class NumericIndexEntry implements DataIndexEntry {
    /** Type of index */
    private final char type;
    /** Id of the index associated with the specified type */
    private final long id;

    /**
     * Constructor
     *
     * @param type type of index (for now 'S' for superstep, or 'P' for
     *             partition)
     * @param id id of the index associated with the given type
     */
    private NumericIndexEntry(char type, long id) {
      this.type = type;
      this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NumericIndexEntry)) {
        return false;
      }
      NumericIndexEntry index = (NumericIndexEntry) obj;
      return index.type == type && index.id == id;
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = result * 37 + type;
      result = result * 37 + (int) id;
      result = result * 37 + (int) (id >> 32);
      return result;
    }

    @Override
    public String toString() {
      return String.format("_%c%d", type, id);
    }

    /**
     * Create a data index entry for a given partition
     *
     * @param partitionId id of the partition
     * @return data index entry for a given partition
     */
    public static NumericIndexEntry createPartitionEntry(int partitionId) {
      return new NumericIndexEntry('P', partitionId);
    }

    /**
     * Create a data index entry for a given superstep
     *
     * @param superstep the superstep number
     * @return data index entry for a given superstep
     */
    public static NumericIndexEntry createSuperstepEntry(long superstep) {
      return new NumericIndexEntry('S', superstep);
    }
  }
}
