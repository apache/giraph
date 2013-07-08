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

package org.apache.giraph.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.giraph.combiner.FloatSumCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphTypes;
import org.apache.giraph.edge.IntNullArrayEdges;
import org.apache.giraph.io.formats.PseudoRandomInputFormatConstants;
import org.apache.giraph.io.formats.PseudoRandomIntNullVertexInputFormat;
import org.apache.giraph.jython.DeployType;
import org.apache.giraph.jython.JythonUtils;
import org.apache.giraph.utils.DistributedCacheUtils;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Sets;

import java.util.Set;

import static org.apache.giraph.jython.JythonComputationFactory.JYTHON_DEPLOY_TYPE;

/**
 * Benchmark for {@link PageRankComputation}
 */
public class PageRankBenchmark extends GiraphBenchmark {
  @Override
  public Set<BenchmarkOption> getBenchmarkOptions() {
    return Sets.newHashSet(BenchmarkOption.VERTICES,
        BenchmarkOption.EDGES_PER_VERTEX, BenchmarkOption.SUPERSTEPS,
        BenchmarkOption.LOCAL_EDGES_MIN_RATIO, BenchmarkOption.JYTHON,
        BenchmarkOption.SCRIPT_PATH);
  }

  @Override
  protected void prepareConfiguration(GiraphConfiguration conf,
      CommandLine cmd) {
    if (BenchmarkOption.JYTHON.optionTurnedOn(cmd)) {
      GiraphTypes types = new GiraphTypes();
      types.inferFrom(PageRankComputation.class);
      String script;
      if (BenchmarkOption.SCRIPT_PATH.optionTurnedOn(cmd)) {
        JYTHON_DEPLOY_TYPE.set(conf, DeployType.DISTRIBUTED_CACHE);
        String path = BenchmarkOption.SCRIPT_PATH.getOptionValue(cmd);
        Path hadoopPath = new Path(path);
        Path remotePath = DistributedCacheUtils.copyAndAdd(hadoopPath, conf);
        script = remotePath.toString();
      } else {
        JYTHON_DEPLOY_TYPE.set(conf, DeployType.RESOURCE);
        script = ReflectionUtils.getPackagePath(this) + "/page-rank.py";
      }
      types.writeIfUnset(conf);
      JythonUtils.init(conf, script, "PageRank");
    } else {
      conf.setComputationClass(PageRankComputation.class);
    }
    conf.setOutEdgesClass(IntNullArrayEdges.class);
    conf.setCombinerClass(FloatSumCombiner.class);
    conf.setVertexInputFormatClass(
        PseudoRandomIntNullVertexInputFormat.class);

    conf.setInt(PseudoRandomInputFormatConstants.AGGREGATE_VERTICES,
        BenchmarkOption.VERTICES.getOptionIntValue(cmd));
    conf.setInt(PseudoRandomInputFormatConstants.EDGES_PER_VERTEX,
        BenchmarkOption.EDGES_PER_VERTEX.getOptionIntValue(cmd));
    conf.setInt(PageRankComputation.SUPERSTEP_COUNT,
        BenchmarkOption.SUPERSTEPS.getOptionIntValue(cmd));
    conf.setFloat(PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
        BenchmarkOption.LOCAL_EDGES_MIN_RATIO.getOptionFloatValue(cmd,
            PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT));
  }

  /**
   * Execute the benchmark.
   *
   * @param args Typically the command line arguments.
   * @throws Exception Any exception from the computation.
   */
  public static void main(final String[] args) throws Exception {
    System.exit(ToolRunner.run(new PageRankBenchmark(), args));
  }
}
