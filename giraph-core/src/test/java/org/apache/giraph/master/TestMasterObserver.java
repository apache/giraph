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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IntNullNullTextInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.util.StringUtils.arrayToString;
import static org.junit.Assert.assertEquals;

public class TestMasterObserver {
  public static class NoOpVertex extends Vertex<IntWritable, NullWritable,
      NullWritable, NullWritable> {
    private int count = 0;

    @Override
    public void compute(Iterable<NullWritable> messages) throws IOException {
      if (count == 2) {
        voteToHalt();
      }
      ++count;
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
    conf.setVertexClass(NoOpVertex.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setVertexInputFormatClass(IntNullNullTextInputFormat.class);
    InternalVertexRunner.run(conf, graph);

    assertEquals(2, Obs.preApp);
    // 3 supersteps + 1 input superstep * 2 observers = 8 callbacks
    assertEquals(8, Obs.preSuperstep);
    assertEquals(8, Obs.postSuperstep);
    assertEquals(2, Obs.postApp);
  }
}
