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
package org.apache.giraph.jython;

import org.apache.giraph.comm.messages.InMemoryMessageStoreFactory;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.GiraphTypes;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.graph.Language;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.scripting.DeployType;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.collect.Maps;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestJythonComputation {

  @Test
  public void testCountEdgesInMemoryMessageStoreFactory() throws Exception {
    testCountEdges(InMemoryMessageStoreFactory.class);
  }

  public void testCountEdges(Class<? extends MessageStoreFactory>
  messageStoreFactoryClass) throws Exception {
    String[] edges = new String[] {
        "1 2",
        "2 3",
        "2 4",
        "4 1"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphTypes types = new GiraphTypes(IntWritable.class, IntWritable.class,
        NullWritable.class, NullWritable.class, NullWritable.class);
    types.writeIfUnset(conf);
    ScriptLoader.setScriptsToLoad(conf,
        "org/apache/giraph/jython/count-edges.py",
        DeployType.RESOURCE, Language.JYTHON);
    JythonUtils.init(conf, "CountEdges");
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    GiraphConstants.MESSAGE_STORE_FACTORY_CLASS.set(conf,
        messageStoreFactoryClass);
    Iterable<String> results = InternalVertexRunner.run(conf, null, edges);

    Map<Integer, Integer> values = parseResults(results);

    // Check that all vertices with outgoing edges have been created
    assertEquals(3, values.size());
    // Check the number of edges for each vertex
    assertEquals(1, (int) values.get(1));
    assertEquals(2, (int) values.get(2));
    assertEquals(1, (int) values.get(4));
  }

  private static Map<Integer, Integer> parseResults(Iterable<String> results) {
    Map<Integer, Integer> values = Maps.newHashMap();
    for (String line : results) {
      String[] tokens = line.split("\\s+");
      int id = Integer.valueOf(tokens[0]);
      int value = Integer.valueOf(tokens[1]);
      values.put(id, value);
    }
    return values;
  }
}
