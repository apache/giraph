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

package org.apache.giraph.rexster.io.formats;

import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_E_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GREMLIN_V_SCRIPT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_HOSTNAME;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_PORT;
import static org.apache.giraph.rexster.conf.GiraphRexsterConstants.GIRAPH_REXSTER_GRAPH;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestRexsterLongDoubleFloatVertexInputFormat
  extends TestAbstractRexsterInputFormat {

  @Test
  public void getEmptyDb() throws Exception {
    Iterable<String>    results;
    GiraphConfiguration conf    = new GiraphConfiguration();
    final String        dbName  = super.DATABASES[0];
    Iterator<String>    result;

    GIRAPH_REXSTER_HOSTNAME.set(conf, "127.0.0.1");
    GIRAPH_REXSTER_PORT.set(conf, 18182);
    GIRAPH_REXSTER_GRAPH.set(conf, dbName);
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(RexsterLongDoubleFloatVertexInputFormat.class);
    conf.setEdgeInputFormatClass(RexsterLongFloatEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);
    results = InternalVertexRunner.run(conf, new String[0], new String[0]);
    Assert.assertNotNull(results);

    result = results.iterator();
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void getTestDb() throws Exception {
    Iterable<String>    results;
    GiraphConfiguration conf    = new GiraphConfiguration();
    final String        dbName  = super.DATABASES[1];
    Iterator<String>    result;
    Iterator<String>    expected;
    final File          expectedFile;
    final URL           expectedFileUrl;

    GIRAPH_REXSTER_HOSTNAME.set(conf, "127.0.0.1");
    GIRAPH_REXSTER_PORT.set(conf, 18182);
    GIRAPH_REXSTER_GRAPH.set(conf, dbName);
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(RexsterLongDoubleFloatVertexInputFormat.class);
    conf.setEdgeInputFormatClass(RexsterLongFloatEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);

    results = InternalVertexRunner.run(conf, new String[0], new String[0]);
    Assert.assertNotNull(results);

    expectedFileUrl =
      this.getClass().getResource(dbName + super.OUTPUT_JSON_EXT);
    expectedFile = new File(expectedFileUrl.toURI());
    expected = Files.readLines(expectedFile, Charsets.UTF_8).iterator();
    result   = results.iterator();

    while(expected.hasNext() && result.hasNext()) {
      String resultLine   = (String) result.next();
      String expectedLine = (String) expected.next();

      Assert.assertTrue(expectedLine.equals(resultLine));
    }
  }

  @Test
  public void getGremlinDb() throws Exception {
    Iterable<String>    results;
    GiraphConfiguration conf    = new GiraphConfiguration();
    final String        dbName  = super.DATABASES[1];
    Iterator<String>    result;
    Iterator<String>    expected;
    final File          expectedFile;
    final URL           expectedFileUrl;

    GIRAPH_REXSTER_HOSTNAME.set(conf, "127.0.0.1");
    GIRAPH_REXSTER_PORT.set(conf, 18182);
    GIRAPH_REXSTER_GRAPH.set(conf, dbName);
    GIRAPH_REXSTER_GREMLIN_V_SCRIPT.set(conf, "g.V");
    GIRAPH_REXSTER_GREMLIN_E_SCRIPT.set(conf, "g.E");
    conf.setComputationClass(EmptyComputation.class);
    conf.setVertexInputFormatClass(RexsterLongDoubleFloatVertexInputFormat.class);
    conf.setEdgeInputFormatClass(RexsterLongFloatEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);

    results = InternalVertexRunner.run(conf, new String[0], new String[0]);
    Assert.assertNotNull(results);

    expectedFileUrl =
      this.getClass().getResource(dbName + super.OUTPUT_JSON_EXT);
    expectedFile = new File(expectedFileUrl.toURI());
    expected = Files.readLines(expectedFile, Charsets.UTF_8).iterator();
    result   = results.iterator();

    while(expected.hasNext() && result.hasNext()) {
      String resultLine   = (String) result.next();
      String expectedLine = (String) expected.next();

      Assert.assertTrue(expectedLine.equals(resultLine));
    }
  }

  /*
  Test compute method that sends each edge a notification of its parents.
  The test set only has a 1-1 parent-to-child ratio for this unit test.
   */
  public static class EmptyComputation
    extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
      vertex.voteToHalt();
    }
  }
}
