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
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_DATASTORE_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_KEY_CLASS;
import static org.apache.giraph.io.gora.constants.GiraphGoraConstants.GIRAPH_GORA_OUTPUT_PERSISTENT_CLASS;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for Gora edge output formats.
 */
public class TestGoraEdgeOutputFormat {

  @Test
  public void getWritingDb() throws Exception {
    Iterable<String>    results;
    GiraphConfiguration conf    = new GiraphConfiguration();
    // Parameters for input
    GIRAPH_GORA_DATASTORE_CLASS.
    set(conf, "org.apache.gora.memory.store.MemStore");
    GIRAPH_GORA_KEYS_FACTORY_CLASS.
    set(conf,"org.apache.giraph.io.gora.utils.DefaultKeyFactory");
    GIRAPH_GORA_KEY_CLASS.set(conf,"java.lang.String");
    GIRAPH_GORA_PERSISTENT_CLASS.
    set(conf,"org.apache.giraph.io.gora.generated.GEdge");
    GIRAPH_GORA_START_KEY.set(conf,"1");
    GIRAPH_GORA_END_KEY.set(conf,"4");
    conf.set("io.serializations",
        "org.apache.hadoop.io.serializer.WritableSerialization," +
        "org.apache.hadoop.io.serializer.JavaSerialization");
    conf.setComputationClass(EmptyComputation.class);
    conf.setEdgeInputFormatClass(GoraTestEdgeInputFormat.class);
    // Parameters for output
    GIRAPH_GORA_OUTPUT_DATASTORE_CLASS.
    set(conf, "org.apache.gora.memory.store.MemStore");
    GIRAPH_GORA_OUTPUT_KEY_CLASS.set(conf, "java.lang.String");
    GIRAPH_GORA_OUTPUT_PERSISTENT_CLASS.
    set(conf,"org.apache.giraph.io.gora.generated.GEdgeResult");
    conf.setEdgeOutputFormatClass(GoraTestEdgeOutputFormat.class);
    results = InternalVertexRunner.run(conf, new String[0], new String[0]);
    Assert.assertNotNull(results);
  }

  /*
  Test compute method that sends each edge a notification of its parents.
  The test set only has a 1-1 parent-to-child ratio for this unit test.
   */
  public static class EmptyComputation
    extends BasicComputation<LongWritable, DoubleWritable,
    FloatWritable, LongWritable> {

    @Override
    public void compute(
        Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
      Assert.assertNotNull(vertex);
      vertex.voteToHalt();
    }
  }
}
