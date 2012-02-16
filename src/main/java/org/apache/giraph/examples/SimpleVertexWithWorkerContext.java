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

package org.apache.giraph.examples;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.examples.SimpleSuperstepVertex.
  SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Fully runnable example of how to
 * emit worker data to HDFS during a graph
 * computation.
 */
public class SimpleVertexWithWorkerContext extends
    EdgeListVertex<LongWritable, IntWritable, FloatWritable, DoubleWritable>
    implements Tool {
  /** Directory name of where to write. */
  public static final String OUTPUTDIR = "svwwc.outputdir";
  /** Halting condition for the number of supersteps */
  private static final int TESTLENGTH = 30;

  @Override
  public void compute(Iterator<DoubleWritable> msgIterator)
    throws IOException {

    long superstep = getSuperstep();

    if (superstep < TESTLENGTH) {
      EmitterWorkerContext emitter =
          (EmitterWorkerContext) getWorkerContext();
      emitter.emit("vertexId=" + getVertexId() +
          " superstep=" + superstep + "\n");
    } else {
      voteToHalt();
    }
  }

  /**
   * Example worker context to emit data as part of a superstep.
   */
  @SuppressWarnings("rawtypes")
  public static class EmitterWorkerContext extends WorkerContext {
    /** File name prefix */
    private static final String FILENAME = "emitter_";
    /** Output stream to dump the strings. */
    private DataOutputStream out;

    @Override
    public void preApplication() {
      Context context = getContext();
      FileSystem fs;

      try {
        fs = FileSystem.get(context.getConfiguration());

        String p = context.getConfiguration()
            .get(SimpleVertexWithWorkerContext.OUTPUTDIR);
        if (p == null) {
          throw new IllegalArgumentException(
              SimpleVertexWithWorkerContext.OUTPUTDIR +
              " undefined!");
        }

        Path path = new Path(p);
        if (!fs.exists(path)) {
          throw new IllegalArgumentException(path +
              " doesn't exist");
        }

        Path outF = new Path(path, FILENAME +
            context.getTaskAttemptID());
        if (fs.exists(outF)) {
          throw new IllegalArgumentException(outF +
              " aready exists");
        }

        out = fs.create(outF);
      } catch (IOException e) {
        throw new RuntimeException(
            "can't initialize WorkerContext", e);
      }
    }

    @Override
    public void postApplication() {
      if (out != null) {
        try {
          out.flush();
          out.close();
        } catch (IOException e) {
          throw new RuntimeException(
              "can't finalize WorkerContext", e);
        }
        out = null;
      }
    }

    @Override
    public void preSuperstep() { }

    @Override
    public void postSuperstep() { }

    /**
     * Write this string to the output stream.
     *
     * @param s String to dump.
     */
    public void emit(String s) {
      try {
        out.writeUTF(s);
      } catch (IOException e) {
        throw new RuntimeException("can't emit", e);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "run: Must have 2 arguments <output path> <# of workers>");
    }
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.setVertexClass(getClass());
    job.setVertexInputFormatClass(
        SimpleSuperstepVertexInputFormat.class);
    job.setWorkerContextClass(EmitterWorkerContext.class);
    Configuration conf = job.getConfiguration();
    conf.set(SimpleVertexWithWorkerContext.OUTPUTDIR, args[0]);
    job.setWorkerConfiguration(Integer.parseInt(args[1]),
        Integer.parseInt(args[1]),
        100.0f);
    if (job.run(true)) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Executable from the command line.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SimpleVertexWithWorkerContext(), args));
  }
}
