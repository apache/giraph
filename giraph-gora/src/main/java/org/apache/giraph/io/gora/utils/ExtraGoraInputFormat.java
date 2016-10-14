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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapReduceUtils;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStore;
import org.apache.gora.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat to fetch the input from Gora data stores. The
 * query to fetch the items from the datastore should be prepared and
 * set via setQuery(Job, Query), before submitting the job.
 *
 * Hadoop jobs can be either configured through static
 *<code>setInput()</code> methods, or from GoraMapper.
 * @param <K> KeyClass.
 * @param <T> PersistentClass.
 */
public class ExtraGoraInputFormat<K, T extends PersistentBase>
  extends InputFormat<K, T> {

  /**
   * String used to map partitioned queries into configuration object.
   */
  public static final String QUERY_KEY = "gora.inputformat.query";

  /**
   * Data store to be used.
   */
  private DataStore<K, T> dataStore;

  /**
   * Query to be performed.
   */
  private Query<K, T> query;

  /**
   * @param split InputSplit to be used.
   * @param context JobContext to be used.
   * @return RecordReader record reader used inside Hadoop job.
   */
  @Override
  @SuppressWarnings("unchecked")
  public RecordReader<K, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {

    PartitionQuery<K, T> partitionQuery = (PartitionQuery<K, T>)
        ((GoraInputSplit) split).getQuery();

    //setInputPath(partitionQuery, context);
    return new GoraRecordReader<K, T>(partitionQuery, context);
  }

  /**
   * Gets splits.
   * @param context for the job.
   * @return splits found
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    List<PartitionQuery<K, T>> queries =
        getDataStore().getPartitions(getQuery());
    List<InputSplit> splits = new ArrayList<InputSplit>(queries.size());
    for (PartitionQuery<K, T> partQuery : queries) {
      ((PartitionQueryImpl) partQuery).setConf(context.getConfiguration());
      splits.add(new GoraInputSplit(context.getConfiguration(), partQuery));
    }
    return splits;
  }

  /**
   * @return the dataStore
   */
  public DataStore<K, T> getDataStore() {
    return dataStore;
  }

  /**
   * @param datStore the dataStore to set
   */
  public void setDataStore(DataStore<K, T> datStore) {
    this.dataStore = datStore;
  }

  /**
   * @return the query
   */
  public Query<K, T> getQuery() {
    return query;
  }

  /**
   * @param query the query to set
   */
  public void setQuery(Query<K, T> query) {
    this.query = query;
  }

  /**
   * Sets the partitioned query inside the job object.
   * @param conf Configuration used.
   * @param query Query to be executed.
   * @param <K> Key class
   * @param <T> Persistent class
   * @throws IOException Exception that be might thrown.
   */
  public static <K, T extends Persistent> void setQuery(Configuration conf,
      Query<K, T> query) throws IOException {
    IOUtils.storeToConf(query, conf, QUERY_KEY);
  }

  /**
   * Gets the partitioned query from the conf object passed.
   * @param conf Configuration object.
   * @return passed inside the configuration object
   * @throws IOException Exception that might be thrown.
   */
  public Query<K, T> getQuery(Configuration conf) throws IOException {
    return IOUtils.loadFromConf(conf, QUERY_KEY);
  }

  /**
   * Sets the input parameters for the job
   * @param job the job to set the properties for
   * @param query the query to get the inputs from
   * @param dataStore the datastore as the input
   * @param reuseObjects whether to reuse objects in serialization
   * @param <K> Key class
   * @param <V> Persistent class
   * @throws IOException
   */
  public static <K, V extends Persistent> void setInput(Job job,
  Query<K, V> query, DataStore<K, V> dataStore, boolean reuseObjects)
    throws IOException {

    Configuration conf = job.getConfiguration();

    GoraMapReduceUtils.setIOSerializations(conf, reuseObjects);

    job.setInputFormatClass(ExtraGoraInputFormat.class);
    ExtraGoraInputFormat.setQuery(job.getConfiguration(), query);
  }
}
