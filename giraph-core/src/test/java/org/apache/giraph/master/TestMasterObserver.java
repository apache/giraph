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

package org.apache.giraph.master;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.util.StringUtils.arrayToString;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;

public class TestMasterObserver {
  public static class SimpleComputation extends BasicComputation<IntWritable,
      IntWritable, NullWritable, NullWritable> {
    @Override
    public void compute(
        Vertex<IntWritable, IntWritable, NullWritable> vertex,
        Iterable<NullWritable> messages) throws IOException {
      int currentValue = vertex.getValue().get();
      if (currentValue == 2) {
        vertex.voteToHalt();
      }
      vertex.setValue(new IntWritable(currentValue + 1));
    }
  }

  public static class InputFormat extends TextVertexInputFormat<
      IntWritable, IntWritable, NullWritable> {
    @Override
    public TextVertexReader createVertexReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new TextVertexReaderFromEachLine() {
        @Override
        protected IntWritable getId(Text line) throws IOException {
          return new IntWritable(Integer.parseInt(line.toString()));
        }

        @Override
        protected IntWritable getValue(Text line) throws IOException {
          return new IntWritable(0);
        }

        @Override
        protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
            Text line) throws IOException {
          return ImmutableList.of();
        }
      };
    }
  }

  public static class Obs extends DefaultMasterObserver {
    public static int preApp = 0;
    public static int preSuperstep = 0;
    public static int postSuperstep = 0;
    public static int postApp = 0;

    @Override
    public void preApplication() {
      ++preApp;
    }

    @Override
    public void postApplication() {
      ++postApp;
    }

    @Override
    public void preSuperstep(long superstep) {
      ++preSuperstep;
    }

    @Override
    public void postSuperstep(long superstep) {
      ++postSuperstep;
    }
  }

  @Test
  public void testGetsCalled() throws Exception {
    assertEquals(0, Obs.postApp);

    String[] graph = new String[] { "1", "2", "3" };

    String klasses[] = new String[] {
        Obs.class.getName(),
        Obs.class.getName()
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.set(GiraphConstants.MASTER_OBSERVER_CLASSES.getKey(),
        arrayToString(klasses));
    conf.setComputationClass(SimpleComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(InputFormat.class);
    InternalVertexRunner.run(conf, graph);

    assertEquals(2, Obs.preApp);
    // 3 supersteps + 1 input superstep * 2 observers = 8 callbacks
    assertEquals(8, Obs.preSuperstep);
    assertEquals(8, Obs.postSuperstep);
    assertEquals(2, Obs.postApp);
  }
}
