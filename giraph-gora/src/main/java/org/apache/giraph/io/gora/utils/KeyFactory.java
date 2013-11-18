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
package org.apache.giraph.io.gora.utils;

import org.apache.gora.store.DataStore;

/**
 * Class used to convert strings into more complex keys.
 */
public abstract class KeyFactory {

  /**
   * Data store used for creating a new key.
   */
  private DataStore dataStore;

  /**
   * Builds a key from a string parameter.
   * @param keyString the key object as a string.
   * @return the key object.
   */
  public abstract Object buildKey(String keyString);

  /**
   * Gets the data store used in this factory.
   * @return the dataStore
   */
  public DataStore getDataStore() {
    return dataStore;
  }

  /**
   * Sets the data store used in this factory.
   * @param dataStore the dataStore to set
   */
  public void setDataStore(DataStore dataStore) {
    this.dataStore = dataStore;
  }
}
