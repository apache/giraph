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

import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;

/**
 * Class used to handle the creation and querying of data stores through Gora.
 */
public class GoraUtils {

  /**
   * Attribute handling the specific class to be created.
   */
  private static Class<? extends DataStore> DATASTORECLASS;

  /**
   * The default constructor is set to be private by default so that the
   * class is not instantiated.
   */
  private GoraUtils() { /* private constructor */ }

  /**
   * Creates a generic data store using the data store class.
   * set using the class property
   * @param conf Configuration
   * @param <K> key class
   * @param <T> value class
   * @param keyClass key class used
   * @param persistentClass persistent class used
   * @return created data store
   * @throws GoraException exception threw
   */
  @SuppressWarnings("unchecked")
  public static <K, T extends Persistent> DataStore<K, T>
  createDataStore(Configuration conf,
      Class<K> keyClass, Class<T> persistentClass)
    throws GoraException {
    DataStoreFactory.createProps();
    DataStore<K, T> dataStore =
        DataStoreFactory.createDataStore((Class<? extends DataStore<K, T>>)
                                          DATASTORECLASS,
                                          keyClass, persistentClass,
                                          conf);

    return dataStore;
  }

  /**
   * Creates a specific data store specified by.
   * @param conf Configuration
   * @param <K> key class
   * @param <T> value class
   * @param dataStoreClass  Defines the type of data store used.
   * @param keyClass  Handles the key class to be used.
   * @param persistentClass Handles the persistent class to be used.
   * @return DataStore created using parameters passed.
   * @throws GoraException  if an error occurs.
   */
  public static <K, T extends Persistent> DataStore<K, T>
  createSpecificDataStore(Configuration conf,
      Class<? extends DataStore> dataStoreClass,
      Class<K> keyClass, Class<T> persistentClass) throws GoraException {
    DATASTORECLASS = dataStoreClass;
    return createDataStore(conf, keyClass, persistentClass);
  }

  /**
   * Performs a range query to Gora datastores
   * @param <K> key class
   * @param <T> value class
   * @param pDataStore  data store being used.
   * @param pStartKey start key for the range query.
   * @param pEndKey end key for the range query.
   * @return Result containing all results for the query.
   */
  public static <K, T extends Persistent> Result<K, T>
  getRequest(DataStore<K, T> pDataStore, K pStartKey, K pEndKey) {
    QueryBase query = getQuery(pDataStore, pStartKey, pEndKey);
    return getRequest(pDataStore, query);
  }

  /**
   * Performs a query to Gora datastores
   * @param pDataStore data store being used.
   * @param query query executed over data stores.
   * @param <K> key class
   * @param <T> value class
   * @return Result containing all results for the query.
   */
  public static <K, T extends Persistent> Result<K, T>
  getRequest(DataStore<K, T> pDataStore, Query<K, T> query) {
    return pDataStore.execute(query);
  }

  /**
   * Performs a range query to Gora datastores
   * @param <K> key class
   * @param <T> value class
   * @param pDataStore  data store being used.
   * @param pStartKey start key for the range query.
   * @return  Result containing all results for the query.
   */
  public static <K, T extends Persistent> Result<K, T>
  getRequest(DataStore<K, T> pDataStore, K pStartKey) {
    return getRequest(pDataStore, pStartKey, null);
  }

  /**
   * Gets a query object to be used as a range query.
   * @param pDataStore data store used.
   * @param pStartKey range start key.
   * @param pEndKey range end key.
   * @param <K> key class
   * @param <T> value class
   * @return range query object.
   */
  public static <K, T extends Persistent> QueryBase
  getQuery(DataStore pDataStore, K pStartKey, K pEndKey) {
    QueryBase query = (QueryBase) pDataStore.newQuery();
    query.setStartKey(pStartKey);
    query.setEndKey(pEndKey);
    return query;
  }

  /**
   * Gets a query object to be used as a simple get.
   * @param pDataStore data store used.
   * @param pStartKey range start key.
   * @param <K> key class
   * @param <T> value class
   * @return query object.
   */
  public static <K, T extends Persistent> Query<K, T>
  getQuery(DataStore<K, T> pDataStore, K pStartKey) {
    Query<K, T> query = pDataStore.newQuery();
    query.setStartKey(pStartKey);
    query.setEndKey(null);
    return query;
  }

  /**
   * Gets a query object to be used as a simple get.
   * @param pDataStore data store used.
   * @param <K> key class
   * @param <T> value class
   * @return query object.
   */
  public static <K, T extends Persistent> Query<K, T>
  getQuery(DataStore<K, T> pDataStore) {
    Query<K, T> query = pDataStore.newQuery();
    query.setStartKey(null);
    query.setEndKey(null);
    return query;
  }
}
