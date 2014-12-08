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
package org.apache.giraph.debugger.instrumenter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.debugger.utils.AggregatedValueWrapper;
import org.apache.giraph.debugger.utils.BaseWrapper;
import org.apache.giraph.debugger.utils.CommonVertexMasterContextWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Common class used by both {@link AbstractInterceptingComputation} and
 * {@link AbstractInterceptingMasterCompute}. Serves following functions:
 * <ul>
 * <li>Maintains a {@link CommonVertexMasterContextWrapper} which contains
 * common information captured by both the Master and the Vertex class, such as
 * aggregators that the user accesses, superstepNo, totalNumberOfVertices and
 * edges.
 * <li>Contains helper methods to save a master or vertex trace file to HDFS and
 * maintains a {@link FileSystem} object that can be used to write other traces
 * to HDFS.
 * <li>Contains a helper method to return the trace directory for a particular
 * job.
 * </ul>
 *
 * TODO: We might consider adding a method to {@link AbstractComputation} and
 * {@link MasterCompute} to return all registered aggregators, such as
 * getAllRegisteredAggregators. Right now we do not intercept aggregators that
 * were never called.
 */
@SuppressWarnings("rawtypes")
public class CommonVertexMasterInterceptionUtil {
  /**
   * Logger for this class.
   */
  private static final Logger LOG = Logger
    .getLogger(CommonVertexMasterInterceptionUtil.class);
  /**
   * The HDFS file system instance to load and save data for debugging.
   */
  private static FileSystem FILE_SYSTEM = null;
  /**
   * The Giraph job id of the job being debugged.
   */
  private final String jobId;
  /**
   * A list of Giraph aggregator values.
   */
  private List<AggregatedValueWrapper> previousAggregatedValueWrappers;
  /**
   * The master context being captured.
   */
  private CommonVertexMasterContextWrapper commonVertexMasterContextWrapper;

  /**
   * Constructs a new instance for the given job.
   *
   * Warning: Caller's should create a new object at least once each superstep.
   *
   * @param jobId The job id of the job being debugged.
   */
  public CommonVertexMasterInterceptionUtil(String jobId) {
    this.jobId = jobId;
    previousAggregatedValueWrappers = new ArrayList<>();
    if (FILE_SYSTEM == null) {
      try {
        FILE_SYSTEM = FileSystem.get(new Configuration());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Initializes this instance.
   *
   * @param immutableClassesConfig The Giraph configuration.
   * @param superstepNo The superstep number.
   * @param totalNumVertices Total number of vertices at this superstep.
   * @param totalNumEdges  Total number of edges at this superstep.
   */
  public void initCommonVertexMasterContextWrapper(
    ImmutableClassesGiraphConfiguration immutableClassesConfig,
    long superstepNo, long totalNumVertices, long totalNumEdges) {
    this.commonVertexMasterContextWrapper = new
      CommonVertexMasterContextWrapper(
      immutableClassesConfig, superstepNo, totalNumVertices, totalNumEdges);
    commonVertexMasterContextWrapper
      .setPreviousAggregatedValues(previousAggregatedValueWrappers);
  }

  /**
   * Captures value of a Giraph aggregator.
   *
   * @param <A> The aggregator value type.
   * @param name The Giraph aggregator name.
   * @param value The aggregator value to capture.
   */
  public <A extends Writable> void addAggregatedValueIfNotExists(String name,
    A value) {
    if (getPreviousAggregatedValueWrapper(name) == null && value != null) {
      previousAggregatedValueWrappers.add(new AggregatedValueWrapper(name,
        value));
    }
  }

  /**
   * Returns captured values of a Giraph aggregator.
   *
   * @param name The Giraph aggregator name.
   * @return The captured aggregator values.
   */
  private AggregatedValueWrapper getPreviousAggregatedValueWrapper(String name)
  {
    for (AggregatedValueWrapper previousAggregatedValueWrapper :
      previousAggregatedValueWrappers) {
      if (name.equals(previousAggregatedValueWrapper.getKey())) {
        return previousAggregatedValueWrapper;
      }
    }
    return null;
  }

  /**
   * Saves captured scenario.
   *
   * @param masterOrVertexScenarioWrapper The scenario to save.
   * @param fullFileName HDFS path for the saved file.
   */
  public void saveScenarioWrapper(BaseWrapper masterOrVertexScenarioWrapper,
    String fullFileName) {
    try {
      masterOrVertexScenarioWrapper.saveToHDFS(FILE_SYSTEM, fullFileName);
    } catch (IOException e) {
      LOG.error("Could not save the " +
        masterOrVertexScenarioWrapper.getClass().getName() +
        " protobuf trace. IOException was thrown. exceptionMessage: " +
        e.getMessage());
      e.printStackTrace();
    }
  }

  public List<AggregatedValueWrapper> getPreviousAggregatedValueWrappers() {
    return previousAggregatedValueWrappers;
  }

  public void setPreviousAggregatedValueWrappers(
    ArrayList<AggregatedValueWrapper> previousAggregatedValueWrappers) {
    this.previousAggregatedValueWrappers = previousAggregatedValueWrappers;
  }

  public CommonVertexMasterContextWrapper getCommonVertexMasterContextWrapper()
  {
    return commonVertexMasterContextWrapper;
  }

  public FileSystem getFileSystem() {
    return FILE_SYSTEM;
  }

  public String getJobId() {
    return jobId;
  }
}
