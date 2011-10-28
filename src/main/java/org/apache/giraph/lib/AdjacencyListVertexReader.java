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
package org.apache.giraph.lib;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

/**
 * VertexReader that readers lines of text with vertices encoded as adjacency
 * lists and converts each token to the correct type.  For example, a graph
 * with vertices as integers and values as doubles could be encoded as:
 *   1 0.1 2 0.2 3 0.3
 * to represent a vertex named 1, with 0.1 as its value and two edges, to
 * vertices 2 and 3, with edge values of 0.2 and 0.3, respectively.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
abstract class AdjacencyListVertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    TextVertexInputFormat.TextVertexReader<I, V, E> {

  public static final String LINE_TOKENIZE_VALUE = "adj.list.input.delimiter";
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  private String splitValue = null;

  /**
   * Utility for doing any cleaning of each line before it is tokenized.
   */
  public interface LineSanitizer {
    /**
     * Clean string s before attempting to tokenize it.
     */
    public String sanitize(String s);
  }

  private LineSanitizer sanitizer = null;

  AdjacencyListVertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
    super(lineRecordReader);
  }

  AdjacencyListVertexReader(RecordReader<LongWritable, Text> lineRecordReader,
      LineSanitizer sanitizer) {
    super(lineRecordReader);
    this.sanitizer = sanitizer;
  }

  /**
   * Store the Id for this line in an instance of its correct type.
   * @param s Id of vertex from line
   * @param id Instance of Id's type, in which to store its value
   */
  abstract public void decodeId(String s, I id);

  /**
   * Store the value for this line in an instance of its correct type.
   * @param s Value from line
   * @param value Instance of value's type, in which to store its value
   */
  abstract public void decodeValue(String s, V value);

  /**
   * Store an edge from the line into an instance of a correctly typed Edge
   * @param id The edge's id from the line
   * @param value The edge's value from the line
   * @param edge Instance of edge in which to store the id and value
   */
  abstract public void decodeEdge(String id, String value, Edge<I, E> edge);

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean next(MutableVertex<I, V, E, ?> iveMutableVertex)
      throws IOException, InterruptedException {
    if (!getRecordReader().nextKeyValue()) {
        return false;
    }

    Configuration conf = getContext().getConfiguration();
    String line = getRecordReader().getCurrentValue().toString();

    if (sanitizer != null) {
      line = sanitizer.sanitize(line);
    }

    if (splitValue == null) {
      splitValue = conf.get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    String [] values = line.split(splitValue);

    if ((values.length < 2) || (values.length % 2 != 0)) {
      throw new IllegalArgumentException("Line did not split correctly: " + line);
    }

    I vertexId = BspUtils.<I>createVertexIndex(conf);
    decodeId(values[0], vertexId);
    iveMutableVertex.setVertexId(vertexId);

    V value = BspUtils.<V>createVertexValue(conf);
    decodeValue(values[1], value);
    iveMutableVertex.setVertexValue(value);

    int i = 2;
    Edge<I, E> edge = new Edge<I, E>();
    while(i < values.length) {
      decodeEdge(values[i], values[i + 1], edge);
      iveMutableVertex.addEdge(edge.getDestVertexId(), edge.getEdgeValue());
      i += 2;
    }

    return true;
  }
}
