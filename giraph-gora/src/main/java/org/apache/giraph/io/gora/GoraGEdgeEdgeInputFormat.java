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

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.gora.generated.GEdge;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Example implementation of a specific reader for a generated data bean.
 */
public class GoraGEdgeEdgeInputFormat
  extends GoraEdgeInputFormat<LongWritable, FloatWritable> {

  /**
   * Default constructor
   */
  public GoraGEdgeEdgeInputFormat() {
  }

  /**
   * Creates specific vertex reader to be used inside Hadoop.
   * @param split split to be read.
   * @param context JobContext to be used.
   * @return GoraEdgeReader Edge reader to be used by Hadoop.
   */
  @Override
  public GoraEdgeReader createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new GoraGEdgeEdgeReader();
  }

  /**
   * Gora edge reader
   */
  protected class GoraGEdgeEdgeReader extends GoraEdgeReader {

    /** source vertex of the edge */
    private LongWritable sourceId;

    /**
     * Transforms a GoraObject into an Edge object.
     * @param goraObject Object from Gora to be translated.
     * @return Edge Result from transforming the gora object.
     */
    @Override
    protected Edge<LongWritable, FloatWritable> transformEdge
    (Object goraObject) {
      Edge<LongWritable, FloatWritable> edge = null;
      GEdge goraEdge = (GEdge) goraObject;
      this.sourceId = new LongWritable();
      this.sourceId.set(Long.parseLong(goraEdge.getVertexInId().toString()));
      edge = EdgeFactory.create(
          new LongWritable(
              Long.parseLong(goraEdge.getVertexOutId().toString())),
          new FloatWritable(goraEdge.getEdgeWeight()));
      return edge;
    }

    /**
     * Gets the currentSourceId for the edge.
     * @return LongWritable currentSourceId for the edge.
     */
    @Override
    public LongWritable getCurrentSourceId() throws IOException,
        InterruptedException {
      return this.sourceId;
    }
  }
}
