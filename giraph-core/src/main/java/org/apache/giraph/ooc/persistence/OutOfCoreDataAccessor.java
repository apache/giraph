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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface representing data accessor object (DAO) used as persistence layer
 * in out-of-core mechanism.
 * Note: any class implementing this interface should have one and only one
 *       constructor taking one and only one argument of type
 *       <code>ImmutableClassesGiraphConfiguration</code>
 */
public interface OutOfCoreDataAccessor {
  /** Initialize the DAO */
  void initialize();

  /** Shut down the DAO */
  void shutdown();

  /**
   * @return the number of threads involved in data persistence
   */
  int getNumAccessorThreads();

  /**
   * Prepare a wrapper containing <code>DataInput</code> representation for a
   * given thread involved in persistence for a given index chain for data.
   *
   * @param threadId id of the thread involved in persistence
   * @param index index chain of the data to access the serialized data form
   * @return the wrapper for <code>DataInput</code> representation of data
   * @throws IOException
   */
  DataInputWrapper prepareInput(int threadId, DataIndex index)
      throws IOException;

  /**
   * Prepare a wrapper containing <code>DataOutput</code> representation for a
   * given thread involved in persistence for a given index chain for data.
   *
   * @param threadId id of the thread involved in persistence
   * @param index index chain of the data to access the serialized data form
   * @param shouldAppend whether the <code>DataOutput</code> should be used for
   *                     appending to already existing data for the given index
   *                     or the <code>DataOutput</code> should create new
   *                     instance to store serialized data
   * @return the wrapper for <code>DataOutput</code> representation of data
   * @throws IOException
   */
  DataOutputWrapper prepareOutput(int threadId, DataIndex index,
                                  boolean shouldAppend) throws IOException;

  /**
   * Whether the data for the given thread and index chain exists?
   *
   * @param threadId id of the thread involved in persistence
   * @param index index chain used to access the data
   * @return True if the data exists for the given index chain for the given
   *         thread, False otherwise
   */
  boolean dataExist(int threadId, DataIndex index);

  /** Interface to wrap <code>DataInput</code> */
  interface DataInputWrapper {
    /**
     * @return the <code>DataInput</code>, should return the same instance
     * every time it's called (not start from the beginning)
     */
    DataInput getDataInput();

    /**
     * Finalize and close the <code>DataInput</code> used for persistence.
     *
     * @param deleteOnClose whether the source of <code>DataInput</code>
     *                      should be deleted on closing/finalizing
     * @return number of bytes read from <code>DataInput</code> since it was
     *         opened
     */
    long finalizeInput(boolean deleteOnClose);
  }

  /** Interface to warp <code>DataOutput</code> */
  interface DataOutputWrapper {
    /**
     * @return the <code>DataOutput</code>
     */
    DataOutput getDataOutput();

    /**
     * Finalize and close the <code>DataOutput</code> used for persistence.
     *
     * @return number of bytes written to <code>DataOutput</code> since it was
     *         opened
     */
    long finalizeOutput();
  }
}
