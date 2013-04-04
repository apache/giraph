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
package org.apache.giraph.io.formats;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
@SuppressWarnings("rawtypes")
public abstract class AdjacencyListTextVertexInputFormat<I extends
    WritableComparable, V extends Writable, E extends Writable> extends
    TextVertexInputFormat<I, V, E> {
  /** Delimiter for split */
  public static final String LINE_TOKENIZE_VALUE = "adj.list.input.delimiter";
  /** Default delimiter for split */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  /**
   * Utility for doing any cleaning of each line before it is tokenized.
   */
  public interface LineSanitizer {
    /**
     * Clean string s before attempting to tokenize it.
     *
     * @param s String to be cleaned.
     * @return Sanitized string.
     */
    String sanitize(String s);
  }

  @Override
  public abstract AdjacencyListTextVertexReader createVertexReader(
      InputSplit split, TaskAttemptContext context);

  /**
   * Vertex reader associated with {@link AdjacencyListTextVertexInputFormat}.
   */
  protected abstract class AdjacencyListTextVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /** Cached delimiter used for split */
    private String splitValue = null;
    /** Sanitizer from constructor. */
    private final LineSanitizer sanitizer;

    /**
     * Constructor without line sanitizer.
     */
    public AdjacencyListTextVertexReader() {
      this(null);
    }

    /**
     * Constructor with line sanitizer.
     *
     * @param sanitizer Sanitizer to be used.
     */
    public AdjacencyListTextVertexReader(LineSanitizer sanitizer) {
      this.sanitizer = sanitizer;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(inputSplit, context);
      splitValue =
          getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String sanitizedLine;
      if (sanitizer != null) {
        sanitizedLine = sanitizer.sanitize(line.toString());
      } else {
        sanitizedLine = line.toString();
      }
      String [] values = sanitizedLine.split(splitValue);
      if ((values.length < 2) || (values.length % 2 != 0)) {
        throw new IllegalArgumentException(
          "Line did not split correctly: " + line);
      }
      return values;
    }

    @Override
    protected I getId(String[] values) throws IOException {
      return decodeId(values[0]);
    }

    /**
     * Decode the id for this line into an instance of its correct type.
     *
     * @param s Id of vertex from line
     * @return Vertex id
     */
    public abstract I decodeId(String s);

    @Override
    protected V getValue(String[] values) throws IOException {
      return decodeValue(values[1]);
    }


    /**
     * Decode the value for this line into an instance of its correct type.
     *
     * @param s Value from line
     * @return Vertex value
     */
    public abstract V decodeValue(String s);

    @Override
    protected Iterable<Edge<I, E>> getEdges(String[] values) throws
        IOException {
      int i = 2;
      List<Edge<I, E>> edges = Lists.newLinkedList();
      while (i < values.length) {
        edges.add(decodeEdge(values[i], values[i + 1]));
        i += 2;
      }
      return edges;
    }

    /**
     * Decode an edge from the line into an instance of a correctly typed Edge
     *
     * @param id The edge's id from the line
     * @param value The edge's value from the line
     * @return Edge with given target id and value
     */
    public abstract Edge<I, E> decodeEdge(String id, String value);

  }
}
