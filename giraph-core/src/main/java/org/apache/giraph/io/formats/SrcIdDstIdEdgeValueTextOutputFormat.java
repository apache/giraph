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

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.giraph.conf.GiraphConstants.GIRAPH_TEXT_OUTPUT_FORMAT_SEPARATOR;
import static org.apache.giraph.conf.GiraphConstants.GIRAPH_TEXT_OUTPUT_FORMAT_REVERSE;

/**
 * Write out Edge Value with Source and Destination ID, but not the vertex
 * value.
 * This is a demostration output format to show the possibility to separately
 * output edges from vertices.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class SrcIdDstIdEdgeValueTextOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends TextEdgeOutputFormat<I, V, E> {

  @Override
  public TextEdgeWriter createEdgeWriter(TaskAttemptContext context) {
    return new SrcIdDstIdEdgeValueEdgeWriter();
  }

  /**
   * Edge writer used with {@link SrcIdDstIdEdgeValueTextOutputFormat}.
   */
  protected class SrcIdDstIdEdgeValueEdgeWriter<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends TextEdgeWriterToEachLine<I, V, E> {

    /** Saved delimiter */
    private String delimiter;
    /** Cached reserve option */
    private boolean reverseOutput;

    @Override
    public void initialize(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(context);
      delimiter = GIRAPH_TEXT_OUTPUT_FORMAT_SEPARATOR.get(getConf());
      reverseOutput = GIRAPH_TEXT_OUTPUT_FORMAT_REVERSE.get(getConf());
    }

    @Override
    protected Text convertEdgeToLine(I sourceId, V sourceValue, Edge<I, E> edge)
      throws IOException {
      StringBuilder msg = new StringBuilder();
      if (reverseOutput) {
        msg.append(edge.getValue().toString());
        msg.append(delimiter);
        msg.append(edge.getTargetVertexId().toString());
        msg.append(delimiter);
        msg.append(sourceId.toString());
      } else {
        msg.append(sourceId.toString());
        msg.append(delimiter);
        msg.append(edge.getTargetVertexId().toString());
        msg.append(delimiter);
        msg.append(edge.getValue().toString());
      }
      return new Text(msg.toString());
    }
  }
}
