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
package org.apache.giraph.io.gora;

import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_DATASTORE_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_END_KEY;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_KEYS_FACTORY_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_KEY_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_PERSISTENT_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_START_KEY;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.gora.utils.ExtraGoraInputFormat;
import org.apache.giraph.io.gora.utils.GoraUtils;
import org.apache.giraph.io.gora.utils.KeyFactory;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 *  Class which wraps the GoraInputFormat. It's designed
 *  as an extension point to EdgeInputFormat subclasses who wish
 *  to read from Gora data sources.
 *
 *  Works with
 *  {@link GoraVertexOutputFormat}
 *
 * @param <I> vertex id type
 * @param <E>  edge type
 */
public abstract class GoraEdgeInputFormat
  <I extends WritableComparable, E extends Writable>
  extends EdgeInputFormat<I, E> {

  /** Start key for querying Gora data store. */
  private static Object START_KEY;

  /** End key for querying Gora data store. */
  private static Object END_KEY;

  /** Logger for Gora's vertex input format. */
  private static final Logger LOG =
          Logger.getLogger(GoraEdgeInputFormat.class);

  /** KeyClass used for getting data. */
  private static Class<?> KEY_CLASS;

  /** The vertex itself will be used as a value inside Gora. */
  private static Class<? extends Persistent> PERSISTENT_CLASS;

  /** Data store class to be used as backend. */
  private static Class<? extends DataStore> DATASTORE_CLASS;

  /** Class used to transform strings into Keys */
  private static Class<?> KEY_FACTORY_CLASS;

  /** Data store used for querying data. */
  private static DataStore DATA_STORE;

  /** counter for iinput records */
  private static int RECORD_COUNTER = 0;

  /** Delegate Gora input format */
  private static ExtraGoraInputFormat GORA_INPUT_FORMAT =
         new ExtraGoraInputFormat();

  /**
   * @param conf configuration parameters
   */
  public void checkInputSpecs(Configuration conf) {
    String sDataStoreType =
        GIRAPH_GORA_DATASTORE_CLASS.get(getConf());
    String sKeyType =
        GIRAPH_GORA_KEY_CLASS.get(getConf());
    String sPersistentType =
        GIRAPH_GORA_PERSISTENT_CLASS.get(getConf());
    String sKeyFactoryClass =
        GIRAPH_GORA_KEYS_FACTORY_CLASS.get(getConf());
    try {
      Class<?> keyClass = Class.forName(sKeyType);
      Class<?> persistentClass = Class.forName(sPersistentType);
      Class<?> dataStoreClass = Class.forName(sDataStoreType);
      Class<?> keyFactoryClass = Class.forName(sKeyFactoryClass);
      setKeyClass(keyClass);
      setPersistentClass((Class<? extends Persistent>) persistentClass);
      setDatastoreClass((Class<? extends DataStore>) dataStoreClass);
      setKeyFactoryClass(keyFactoryClass);
      setDataStore(createDataStore(getConf()));
      GORA_INPUT_FORMAT.setDataStore(getDataStore());
    } catch (ClassNotFoundException e) {
      LOG.error("Error while reading Gora Input parameters");
      e.printStackTrace();
    }
  }

  /**
   * Gets the splits for a data store.
   * @param context JobContext
   * @param minSplitCountHint Hint for a minimum split count
   * @return A list of splits
   */
  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
    throws IOException, InterruptedException {
    KeyFactory kFact = null;
    try {
      kFact = (KeyFactory) getKeyFactoryClass().newInstance();
    } catch (InstantiationException e) {
      LOG.error("Key factory was not instantiated. Please verify.");
      LOG.error(e.getMessage());
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      LOG.error("Key factory was not instantiated. Please verify.");
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
    String sKey = GIRAPH_GORA_START_KEY.get(getConf());
    String eKey = GIRAPH_GORA_END_KEY.get(getConf());
    if (sKey == null || sKey.isEmpty()) {
      LOG.warn("No start key has been defined.");
      LOG.warn("Querying all the data store.");
      sKey = null;
      eKey = null;
    }
    kFact.setDataStore(getDataStore());
    setStartKey(kFact.buildKey(sKey));
    setEndKey(kFact.buildKey(eKey));
    QueryBase tmpQuery = GoraUtils.getQuery(
        getDataStore(), getStartKey(), getEndKey());
    tmpQuery.setConf(context.getConfiguration());
    GORA_INPUT_FORMAT.setQuery(tmpQuery);
    List<InputSplit> splits = GORA_INPUT_FORMAT.getSplits(context);
    return splits;
  }

  @Override
  public abstract GoraEdgeReader createEdgeReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex input. Easiest to ignore the key value separator and only use
   * key instead.
   */
  protected abstract class GoraEdgeReader extends EdgeReader<I, E> {
    /** current edge obtained from Rexster */
    private Edge<I, E> edge;
    /** Results gotten from Gora data store. */
    private Result readResults;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      getResults();
      RECORD_COUNTER = 0;
    }

    /**
     * Gets the next edge from Gora data store.
     * @return true/false depending on the existence of vertices.
     * @throws IOException exceptions passed along.
     * @throws InterruptedException exceptions passed along.
     */
    @Override
    // CHECKSTYLE: stop IllegalCatch
    public boolean nextEdge() throws IOException, InterruptedException {
      boolean flg = false;
      try {
        flg = this.getReadResults().next();
        this.edge = transformEdge(this.getReadResults().get());
        RECORD_COUNTER++;
      } catch (Exception e) {
        LOG.debug("Error transforming vertices.");
        flg = false;
      }
      LOG.debug(RECORD_COUNTER + " were transformed.");
      return flg;
    }
    // CHECKSTYLE: resume IllegalCatch

    /**
     * Gets the progress of reading results from Gora.
     * @return the progress of reading results from Gora.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
      float progress = 0.0f;
      if (getReadResults() != null) {
        progress = getReadResults().getProgress();
      }
      return progress;
    }

    /**
     * Gets current edge.
     *
     * @return  The edge object represented by a Gora object
     */
    @Override
    public Edge<I, E> getCurrentEdge()
      throws IOException, InterruptedException {
      return this.edge;
    }

    /**
     * Parser for a single Gora object
     *
     * @param   goraObject vertex represented as a GoraObject
     * @return  The edge object represented by a Gora object
     */
    protected abstract Edge<I, E> transformEdge(Object goraObject);

    /**
     * Performs a range query to a Gora data store.
     */
    protected void getResults() {
      setReadResults(GoraUtils.getRequest(getDataStore(),
          getStartKey(), getEndKey()));
    }

    /**
     * Finishes the reading process.
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Gets the results read.
     * @return results read.
     */
    Result getReadResults() {
      return readResults;
    }

    /**
     * Sets the results read.
     * @param readResults results read.
     */
    void setReadResults(Result readResults) {
      this.readResults = readResults;
    }
  }

  /**
   * Gets the data store object initialized.
   * @param conf Configuration
   * @return DataStore created
   */
  public DataStore createDataStore(Configuration conf) {
    DataStore dsCreated = null;
    try {
      dsCreated = GoraUtils.createSpecificDataStore(conf, getDatastoreClass(),
          getKeyClass(), getPersistentClass());
    } catch (GoraException e) {
      LOG.error("Error creating data store.");
      e.printStackTrace();
    }
    return dsCreated;
  }

  /**
   * Gets the persistent Class
   * @return persistentClass used
   */
  static Class<? extends Persistent> getPersistentClass() {
    return PERSISTENT_CLASS;
  }

  /**
   * Sets the persistent Class
   * @param persistentClassUsed to be set
   */
  static void setPersistentClass
  (Class<? extends Persistent> persistentClassUsed) {
    PERSISTENT_CLASS = persistentClassUsed;
  }

  /**
   * Gets the key class used.
   * @return the key class used.
   */
  static Class<?> getKeyClass() {
    return KEY_CLASS;
  }

  /**
   * Sets the key class used.
   * @param keyClassUsed key class used.
   */
  static void setKeyClass(Class<?> keyClassUsed) {
    KEY_CLASS = keyClassUsed;
  }

  /**
   * @return Class the DATASTORE_CLASS
   */
  public static Class<? extends DataStore> getDatastoreClass() {
    return DATASTORE_CLASS;
  }

  /**
   * @param dataStoreClass the dataStore class to set
   */
  public static void setDatastoreClass(
      Class<? extends DataStore> dataStoreClass) {
    DATASTORE_CLASS = dataStoreClass;
  }

  /**
   * Gets the start key for querying.
   * @return the start key.
   */
  public Object getStartKey() {
    return START_KEY;
  }

  /**
   * Gets the start key for querying.
   * @param startKey start key.
   */
  public static void setStartKey(Object startKey) {
    START_KEY = startKey;
  }

  /**
   * Gets the end key for querying.
   * @return the end key.
   */
  static Object getEndKey() {
    return END_KEY;
  }

  /**
   * Sets the end key for querying.
   * @param pEndKey start key.
   */
  static void setEndKey(Object pEndKey) {
    END_KEY = pEndKey;
  }

  /**
   * Gets the key factory class.
   * @return the kEY_FACTORY_CLASS
   */
  static Class<?> getKeyFactoryClass() {
    return KEY_FACTORY_CLASS;
  }

  /**
   * Sets the key factory class.
   * @param keyFactoryClass the keyFactoryClass to set.
   */
  static void setKeyFactoryClass(Class<?> keyFactoryClass) {
    KEY_FACTORY_CLASS = keyFactoryClass;
  }

  /**
   * Gets the data store.
   * @return DataStore
   */
  public static DataStore getDataStore() {
    return DATA_STORE;
  }

  /**
   * Sets the data store
   * @param dStore the dATA_STORE to set
   */
  public static void setDataStore(DataStore dStore) {
    DATA_STORE = dStore;
  }

  /**
   * Returns a logger.
   * @return the log for the output format.
   */
  public static Logger getLogger() {
    return LOG;
  }
}
