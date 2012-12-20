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

package org.apache.giraph.io.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatBaseInputFormat;
import org.apache.hcatalog.mapreduce.HCatSplit;
import org.apache.hcatalog.mapreduce.HCatStorageHandler;
import org.apache.hcatalog.mapreduce.HCatUtils;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.hcatalog.mapreduce.PartInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Provides functionality similar to
 * {@link org.apache.hcatalog.mapreduce.HCatInputFormat},
 * but allows for different data sources (vertex and edge data).
 */
public class GiraphHCatInputFormat extends HCatBaseInputFormat {
  /** Vertex input job info for HCatalog. */
  public static final String VERTEX_INPUT_JOB_INFO =
      "giraph.hcat.vertex.input.job.info";
  /** Edge input job info for HCatalog. */
  public static final String EDGE_INPUT_JOB_INFO =
      "giraph.hcat.edge.input.job.info";

  /**
   * Set vertex {@link InputJobInfo}.
   *
   * @param job The job
   * @param inputJobInfo Vertex input job info
   * @throws IOException
   */
  public static void setVertexInput(Job job,
                                    InputJobInfo inputJobInfo)
    throws IOException {
    InputJobInfo vertexInputJobInfo = InputJobInfo.create(
        inputJobInfo.getDatabaseName(),
        inputJobInfo.getTableName(),
        inputJobInfo.getFilter());
    vertexInputJobInfo.getProperties().putAll(inputJobInfo.getProperties());
    Configuration conf = job.getConfiguration();
    conf.set(VERTEX_INPUT_JOB_INFO, HCatUtil.serialize(
        HCatUtils.getInputJobInfo(conf, vertexInputJobInfo)));
  }

  /**
   * Set edge {@link InputJobInfo}.
   *
   * @param job The job
   * @param inputJobInfo Edge input job info
   * @throws IOException
   */
  public static void setEdgeInput(Job job,
                                  InputJobInfo inputJobInfo)
    throws IOException {
    InputJobInfo edgeInputJobInfo = InputJobInfo.create(
        inputJobInfo.getDatabaseName(),
        inputJobInfo.getTableName(),
        inputJobInfo.getFilter());
    edgeInputJobInfo.getProperties().putAll(inputJobInfo.getProperties());
    Configuration conf = job.getConfiguration();
    conf.set(EDGE_INPUT_JOB_INFO, HCatUtil.serialize(
        HCatUtils.getInputJobInfo(conf, edgeInputJobInfo)));
  }

  /**
   * Get table schema from input job info.
   *
   * @param inputJobInfo Input job info
   * @return Input table schema
   * @throws IOException
   */
  private static HCatSchema getTableSchema(InputJobInfo inputJobInfo)
    throws IOException {
    HCatSchema allCols = new HCatSchema(new LinkedList<HCatFieldSchema>());
    for (HCatFieldSchema field :
        inputJobInfo.getTableInfo().getDataColumns().getFields()) {
      allCols.append(field);
    }
    for (HCatFieldSchema field :
        inputJobInfo.getTableInfo().getPartitionColumns().getFields()) {
      allCols.append(field);
    }
    return allCols;
  }

  /**
   * Get vertex input table schema.
   *
   * @param conf Job configuration
   * @return Vertex input table schema
   * @throws IOException
   */
  public static HCatSchema getVertexTableSchema(Configuration conf)
    throws IOException {
    return getTableSchema(getVertexJobInfo(conf));
  }

  /**
   * Get edge input table schema.
   *
   * @param conf Job configuration
   * @return Edge input table schema
   * @throws IOException
   */
  public static HCatSchema getEdgeTableSchema(Configuration conf)
    throws IOException {
    return getTableSchema(getEdgeJobInfo(conf));
  }

  /**
   * Set input path for job.
   *
   * @param jobConf Job configuration
   * @param location Location of input files
   * @throws IOException
   */
  private void setInputPath(JobConf jobConf, String location)
    throws IOException {
    int length = location.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();

    for (int i = 0; i < length; i++) {
      char ch = location.charAt(i);
      switch (ch) {
      case '{':
        curlyOpen++;
        if (!globPattern) {
          globPattern = true;
        }
        break;
      case '}':
        curlyOpen--;
        if (curlyOpen == 0 && globPattern) {
          globPattern = false;
        }
        break;
      case ',':
        if (!globPattern) {
          pathStrings.add(location.substring(pathStart, i));
          pathStart = i + 1;
        }
        break;
      default:
      }
    }
    pathStrings.add(location.substring(pathStart, length));

    Path[] paths = StringUtils.stringToPath(pathStrings.toArray(new String[0]));

    FileSystem fs = FileSystem.get(jobConf);
    Path path = paths[0].makeQualified(fs);
    StringBuilder str = new StringBuilder(StringUtils.escapeString(
        path.toString()));
    for (int i = 1; i < paths.length; i++) {
      str.append(StringUtils.COMMA_STR);
      path = paths[i].makeQualified(fs);
      str.append(StringUtils.escapeString(path.toString()));
    }

    jobConf.set("mapred.input.dir", str.toString());
  }

  /**
   * Get input splits for job.
   *
   * @param jobContext Job context
   * @param inputJobInfo Input job info
   * @return MapReduce setting for file input directory
   * @throws IOException
   * @throws InterruptedException
   */
  private List<InputSplit> getSplits(JobContext jobContext,
                                     InputJobInfo inputJobInfo)
    throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();

    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<PartInfo> partitionInfoList = inputJobInfo.getPartitions();
    if (partitionInfoList == null) {
      //No partitions match the specified partition filter
      return splits;
    }

    HCatStorageHandler storageHandler;
    JobConf jobConf;
    //For each matching partition, call getSplits on the underlying InputFormat
    for (PartInfo partitionInfo : partitionInfoList) {
      jobConf = HCatUtil.getJobConfFromContext(jobContext);
      setInputPath(jobConf, partitionInfo.getLocation());
      Map<String, String> jobProperties = partitionInfo.getJobProperties();

      HCatSchema allCols = new HCatSchema(new LinkedList<HCatFieldSchema>());
      for (HCatFieldSchema field :
          inputJobInfo.getTableInfo().getDataColumns().getFields()) {
        allCols.append(field);
      }
      for (HCatFieldSchema field :
          inputJobInfo.getTableInfo().getPartitionColumns().getFields()) {
        allCols.append(field);
      }

      HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

      storageHandler = HCatUtil.getStorageHandler(
          jobConf, partitionInfo);

      //Get the input format
      Class inputFormatClass = storageHandler.getInputFormatClass();
      org.apache.hadoop.mapred.InputFormat inputFormat =
          getMapRedInputFormat(jobConf, inputFormatClass);

      //Call getSplit on the InputFormat, create an HCatSplit for each
      //underlying split. When the desired number of input splits is missing,
      //use a default number (denoted by zero).
      //TODO: Currently each partition is split independently into
      //a desired number. However, we want the union of all partitions to be
      //split into a desired number while maintaining balanced sizes of input
      //splits.
      int desiredNumSplits =
          conf.getInt(HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, 0);
      org.apache.hadoop.mapred.InputSplit[] baseSplits =
          inputFormat.getSplits(jobConf, desiredNumSplits);

      for (org.apache.hadoop.mapred.InputSplit split : baseSplits) {
        splits.add(new HCatSplit(partitionInfo, split, allCols));
      }
    }

    return splits;
  }

  /**
   * Get vertex {@link InputJobInfo}.
   *
   * @param conf Configuration
   * @return Vertex input job info
   * @throws IOException
   */
  private static InputJobInfo getVertexJobInfo(Configuration conf)
    throws IOException {
    String jobString = conf.get(VERTEX_INPUT_JOB_INFO);
    if (jobString == null) {
      throw new IOException("Vertex job information not found in JobContext." +
          " GiraphHCatInputFormat.setVertexInput() not called?");
    }
    return (InputJobInfo) HCatUtil.deserialize(jobString);
  }

  /**
   * Get edge {@link InputJobInfo}.
   *
   * @param conf Configuration
   * @return Edge input job info
   * @throws IOException
   */
  private static InputJobInfo getEdgeJobInfo(Configuration conf)
    throws IOException {
    String jobString = conf.get(EDGE_INPUT_JOB_INFO);
    if (jobString == null) {
      throw new IOException("Edge job information not found in JobContext." +
          " GiraphHCatInputFormat.setEdgeInput() not called?");
    }
    return (InputJobInfo) HCatUtil.deserialize(jobString);
  }

  /**
   * Get vertex input splits.
   *
   * @param jobContext Job context
   * @return List of vertex {@link InputSplit}s
   * @throws IOException
   * @throws InterruptedException
   */
  public List<InputSplit> getVertexSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    return getSplits(jobContext,
        getVertexJobInfo(jobContext.getConfiguration()));
  }

  /**
   * Get edge input splits.
   *
   * @param jobContext Job context
   * @return List of edge {@link InputSplit}s
   * @throws IOException
   * @throws InterruptedException
   */
  public List<InputSplit> getEdgeSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    return getSplits(jobContext,
        getEdgeJobInfo(jobContext.getConfiguration()));
  }

  /**
   * Create an {@link org.apache.hcatalog.mapreduce.HCatRecordReader}.
   *
   * @param split Input split
   * @param schema Table schema
   * @param taskContext Context
   * @return Record reader
   * @throws IOException
   * @throws InterruptedException
   */
  private RecordReader<WritableComparable, HCatRecord>
  createRecordReader(InputSplit split,
                     HCatSchema schema,
                     TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
    HCatSplit hcatSplit = HCatUtils.castToHCatSplit(split);
    PartInfo partitionInfo = hcatSplit.getPartitionInfo();
    JobContext jobContext = taskContext;
    Configuration conf = jobContext.getConfiguration();

    HCatStorageHandler storageHandler = HCatUtil.getStorageHandler(
        conf, partitionInfo);

    JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
    Map<String, String> jobProperties = partitionInfo.getJobProperties();
    HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);

    Map<String, String> valuesNotInDataCols = getColValsNotInDataColumns(
        schema, partitionInfo);

    return HCatUtils.newHCatReader(storageHandler, valuesNotInDataCols);
  }

  /**
   * Create a {@link RecordReader} for vertices.
   *
   * @param split Input split
   * @param taskContext Context
   * @return Record reader
   * @throws IOException
   * @throws InterruptedException
   */
  public RecordReader<WritableComparable, HCatRecord>
  createVertexRecordReader(InputSplit split, TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
    return createRecordReader(split, getVertexTableSchema(
        taskContext.getConfiguration()), taskContext);
  }

  /**
   * Create a {@link RecordReader} for edges.
   *
   * @param split Input split
   * @param taskContext Context
   * @return Record reader
   * @throws IOException
   * @throws InterruptedException
   */
  public RecordReader<WritableComparable, HCatRecord>
  createEdgeRecordReader(InputSplit split, TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
    return createRecordReader(split, getEdgeTableSchema(
        taskContext.getConfiguration()), taskContext);
  }

  /**
   * Get values for fields requested by output schema which will not be in the
   * data.
   *
   * @param outputSchema Output schema
   * @param partInfo Partition info
   * @return Values not in data columns
   */
  private static Map<String, String> getColValsNotInDataColumns(
      HCatSchema outputSchema,
      PartInfo partInfo) {
    HCatSchema dataSchema = partInfo.getPartitionSchema();
    Map<String, String> vals = new HashMap<String, String>();
    for (String fieldName : outputSchema.getFieldNames()) {
      if (dataSchema.getPosition(fieldName) == null) {
        // this entry of output is not present in the output schema
        // so, we first check the table schema to see if it is a part col
        if (partInfo.getPartitionValues().containsKey(fieldName)) {
          vals.put(fieldName, partInfo.getPartitionValues().get(fieldName));
        } else {
          vals.put(fieldName, null);
        }
      }
    }
    return vals;
  }
}
