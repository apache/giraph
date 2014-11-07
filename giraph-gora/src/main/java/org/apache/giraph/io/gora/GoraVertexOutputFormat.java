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

import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_DATASTORE_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_KEY_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_PERSISTENT_CLASS;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.gora.utils.GoraUtils;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
/**
 *
 *  Class which wraps the GoraOutputFormat. It's designed
 *  as an extension point to VertexOutputFormat subclasses who wish
 *  to write vertices back to an Accumulo table.
 *
 *  Works with
 *  {@link GoraVertexInputFormat}
 *
 *
 * @param <I> vertex id type
 * @param <V>  vertex value type
 * @param <E>  edge type
 */
public abstract class GoraVertexOutputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexOutputFormat<I, V, E> {

  /** Logger for Gora's vertex input format. */
  private static final Logger LOG =
        Logger.getLogger(GoraVertexOutputFormat.class);

  /** KeyClass used for getting data. */
  private static Class<?> KEY_CLASS;

  /** The vertex itself will be used as a value inside Gora. */
  private static Class<? extends Persistent> PERSISTENT_CLASS;

  /** Data store class to be used as backend. */
  private static Class<? extends DataStore> DATASTORE_CLASS;

  /** Data store used for querying data. */
  private static DataStore DATA_STORE;

  /**
   * checkOutputSpecs
   *
   * @param context information about the job
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void checkOutputSpecs(JobContext context)
    throws IOException, InterruptedException {
  }

  /**
   * Gets the data store object initialized.
   * @param conf Configuration.
   * @return DataStore created
   */
  public DataStore createDataStore(Configuration conf) {
    DataStore dsCreated = null;
    try {
      dsCreated = GoraUtils.createSpecificDataStore(conf, getDatastoreClass(),
          getKeyClass(), getPersistentClass());
    } catch (GoraException e) {
      getLogger().error("Error creating data store.");
      e.printStackTrace();
    }
    return dsCreated;
  }

  /**
   * getOutputCommitter
   *
   * @param context the task context
   * @return OutputCommitter
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }

  /**
   * Empty output commiter for hadoop.
   */
  private static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void abortTask(TaskAttemptContext arg0) throws IOException {    }

    @Override
    public void commitTask(TaskAttemptContext arg0) throws IOException {    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
      return false;
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException {    }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException {    }
  }

  /**
   * Abstract class to be implemented by the user based on their specific
   * vertex/edges output. Easiest to ignore the key value separator and only
   * use key instead.
   */
  protected abstract class GoraVertexWriter
    extends VertexWriter<I, V, E>
    implements Watcher {
    /** lock for management of the barrier */
    private final Object lock = new Object();

    @Override
    public void initialize(TaskAttemptContext context)
      throws IOException, InterruptedException {
      String sDataStoreType =
        GIRAPH_GORA_OUTPUT_DATASTORE_CLASS.get(getConf());
      String sKeyType =
        GIRAPH_GORA_OUTPUT_KEY_CLASS.get(getConf());
      String sPersistentType =
        GIRAPH_GORA_OUTPUT_PERSISTENT_CLASS.get(getConf());
      try {
        Class<?> keyClass = Class.forName(sKeyType);
        Class<?> persistentClass = Class.forName(sPersistentType);
        Class<?> dataStoreClass = Class.forName(sDataStoreType);
        setKeyClass(keyClass);
        setPersistentClass((Class<? extends Persistent>) persistentClass);
        setDatastoreClass((Class<? extends DataStore>) dataStoreClass);
        setDataStore(createDataStore(context.getConfiguration()));
        if (getDataStore() != null) {
          getLogger().info("The output data store has been created.");
        }
      } catch (ClassNotFoundException e) {
        getLogger().error("Error while reading Gora Output parameters");
        e.printStackTrace();
      }
    }

    @Override
    public void close(TaskAttemptContext context)
      throws IOException, InterruptedException {
      getDataStore().flush();
      getDataStore().close();
    }

    @Override
    public void writeVertex(Vertex<I, V, E> vertex)
      throws IOException, InterruptedException {
      Persistent goraVertex = null;
      Object goraKey = getGoraKey(vertex);
      goraVertex = getGoraVertex(vertex);
      getDataStore().put(goraKey, goraVertex);
    }

    @Override
    public void process(WatchedEvent event) {
      EventType type = event.getType();
      if (type == EventType.NodeChildrenChanged) {
        if (getLogger().isDebugEnabled()) {
          getLogger().debug("signal: number of children changed.");
        }
        synchronized (lock) {
          lock.notify();
        }
      }
    }

    /**
     * Each vertex needs to be transformed into a Gora object to be sent to
     * a specific data store.
     *
     * @param  vertex   vertex to be transformed into a Gora object
     * @return          Gora representation of the vertex
     */
    protected abstract Persistent getGoraVertex(Vertex<I, V, E> vertex);

    /**
     * Gets the correct key from a computed vertex.
     * @param vertex  vertex to extract the key from.
     * @return        The key representing such vertex
     */
    protected abstract Object getGoraKey(Vertex<I, V, E> vertex);

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
   * Returns a logger.
   * @return the log for the output format.
   */
  public static Logger getLogger() {
    return LOG;
  }
}
