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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Writes graph to a dot file (graphviz format). This writes it out in parts. At
 * the end of the job you can use the following to get a single graphviz file:
 *
 * hadoop fs -getmerge /hadoop/output/path data.txt
 */
public class GraphvizOutputFormat extends TextVertexOutputFormat<
    WritableComparable, Writable, Writable> {
  /** Color of node text */
  private static final String NODE_TEXT_COLOR = "blue:orange";

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new VertexWriter();
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new GraphvizOutputCommitter(super.getOutputCommitter(context));
  }

  /**
   * Get output directory job is using
   * @param context job context
   * @return Path for output directory
   */

  private static Path getOutputDir(JobContext context) {
    return FileOutputFormat.getOutputPath(context);
  }

  /**
   * Get path which sorts at the beginning of a directory
   * @param context job context
   * @return Path at beginning
   */

  private static Path getPathAtBeginning(JobContext context) {
    return new Path(getOutputDir(context), "____" + System.currentTimeMillis());
  }

  /**
   * Get path which sorts at the end of a directory
   * @param context job context
   * @return Path at end
   */
  private static Path getPathAtEnd(JobContext context) {
    return new Path(getOutputDir(context), "zzz_" + System.currentTimeMillis());
  }

  /**
   * Write start of graph data
   * @param context task attempt context
   * @throws IOException I/O errors
   */
  private static void writeStart(JobContext context) throws IOException {
    Path path = getPathAtBeginning(context);
    FileSystem fs = path.getFileSystem(context.getConfiguration());
    FSDataOutputStream file = fs.create(path, false);
    file.writeBytes("digraph g {\n");
    file.close();
  }

  /**
   * Write end of graph data
   * @param context job attempt context
   * @throws IOException I/O errors
   */
  private static void writeEnd(JobContext context) throws IOException {
    Path path = getPathAtEnd(context);
    FileSystem fs = path.getFileSystem(context.getConfiguration());
    FSDataOutputStream file = fs.create(path, false);
    file.writeBytes("}\n");
    file.close();
  }

  /**
   * Add node information to output
   * @param vertex node
   * @param sb string builder
   */
  private static void addNodeInfo(
      Vertex<WritableComparable, Writable, Writable> vertex, StringBuilder sb) {
    sb.append('"').append(vertex.getId()).append('"');
    sb.append(" [").append("label=").append('"').append("<id> ");
    sb.append(vertex.getId());
    if (!(vertex.getValue() instanceof NullWritable)) {
      sb.append("|").append(vertex.getValue());
    }
    sb.append('"').append(",shape=record,fillcolor=")
        .append('"').append(NODE_TEXT_COLOR).append('"')
        .append("];");
  }

  /**
   * Write an edge
   * @param sb string builder
   * @param sourceID source vertex ID
   * @param edge the edge
   */
  private static void addEdge(StringBuilder sb, Writable sourceID,
      Edge<WritableComparable, Writable> edge) {
    sb.append(sourceID).append(":id")
        .append(" -> ")
        .append(edge.getTargetVertexId()).append(":id");
    addEdgeInfo(sb, edge);
    sb.append("\n");
  }

  /**
   * Add edge information to output
   * @param sb string builder
   * @param edge the edge
   */
  private static void addEdgeInfo(StringBuilder sb,
    Edge<WritableComparable, Writable> edge) {
    if (!(edge.getValue() instanceof NullWritable)) {
      sb.append(" [label=").append(edge.getValue()).append(" ];");
    }
  }

  /**
   * Wrapper around output committer which writes our begin/end files.
   */
  private static class GraphvizOutputCommitter extends OutputCommitter {
    /** delegate committer */
    private final OutputCommitter delegate;

    /**
     * Constructor with delegate
     * @param delegate committer to use
     */
    private GraphvizOutputCommitter(OutputCommitter delegate) {
      this.delegate = delegate;
    }

    @Override public boolean equals(Object o) {
      return delegate.equals(o);
    }

    @Override public String toString() {
      return delegate.toString();
    }

    @Override public int hashCode() {
      return delegate.hashCode();
    }

    @Override public void abortJob(JobContext jobContext, JobStatus.State state)
      throws IOException {
      delegate.abortJob(jobContext, state);
    }

    @Override public void abortTask(TaskAttemptContext taskContext)
      throws IOException {
      delegate.abortTask(taskContext);
    }

    @Override @Deprecated public void cleanupJob(JobContext context)
      throws IOException {
      delegate.cleanupJob(context);
    }

    @Override public void commitJob(JobContext jobContext) throws IOException {
      writeEnd(jobContext);
      delegate.commitJob(jobContext);
    }

    @Override public void commitTask(TaskAttemptContext taskContext)
      throws IOException {
      delegate.commitTask(taskContext);
    }

    @Override public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
      return delegate.needsTaskCommit(taskContext);
    }

    @Override public void setupJob(JobContext jobContext) throws IOException {
      delegate.setupJob(jobContext);
      writeStart(jobContext);
    }

    @Override public void setupTask(TaskAttemptContext taskContext)
      throws IOException {
      delegate.setupTask(taskContext);
    }
  }

  /**
   * Writes vertices to graphviz files.
   */
  private class VertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
      Vertex<WritableComparable, Writable, Writable> vertex)
      throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder(vertex.getNumEdges() * 10);
      for (Edge<WritableComparable, Writable> edge : vertex.getEdges()) {
        addEdge(sb, vertex.getId(), edge);
      }
      addNodeInfo(vertex, sb);
      getRecordWriter().write(new Text(sb.toString()), null);
    }
  }
}
