/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.giraph.examples.GeneratedVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.lib.JsonBase64VertexInputFormat;
import org.apache.giraph.lib.JsonBase64VertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class TestVertexTypes
    extends TestCase {

    /**
     * Matches the {@link GeneratedVertexInputFormat}
     */
    private static class GeneratedVertexMatch extends
            Vertex<LongWritable, IntWritable, FloatWritable, FloatWritable> {
        @Override
        public void compute(Iterator<FloatWritable> msgIterator)
                throws IOException {
        }
    }

    /**
     * Matches the {@link GeneratedVertexInputFormat}
     */
    private static class DerivedVertexMatch extends GeneratedVertexMatch {
    }

    /**
     * Mismatches the {@link GeneratedVertexInputFormat}
     */
    private static class GeneratedVertexMismatch extends
            Vertex<LongWritable, FloatWritable, FloatWritable, FloatWritable> {
        @Override
        public void compute(Iterator<FloatWritable> msgIterator)
                throws IOException {
        }
    }

    /**
     * Matches the {@link GeneratedVertexMatch}
     */
    private static class GeneratedVertexMatchCombiner extends
            VertexCombiner<LongWritable, FloatWritable> {

        @Override
        public FloatWritable combine(LongWritable vertexIndex,
                                  List<FloatWritable> msgList)
                throws IOException {
            return null;
        }
    }

    /**
     * Mismatches the {@link GeneratedVertexMatch}
     */
    private static class GeneratedVertexMismatchCombiner extends
            VertexCombiner<LongWritable, DoubleWritable> {

        @Override
        public DoubleWritable combine(LongWritable vertexIndex,
                                      List<DoubleWritable> msgList)
                throws IOException {
            return null;
        }
    }

    public void testMatchingType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      GeneratedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_COMBINER_CLASS,
                      GeneratedVertexMatchCombiner.class,
                      VertexCombiner.class);
        mapper.determineClassTypes(conf);
    }

    public void testDerivedMatchingType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      DerivedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        mapper.determineClassTypes(conf);
    }

    public void testDerivedInputFormatType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      DerivedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        mapper.determineClassTypes(conf);
    }

    public void testMismatchingVertex() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      GeneratedVertexMismatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        try {
            mapper.determineClassTypes(conf);
            throw new RuntimeException(
                "testMismatchingVertex: Should have caught an exception!");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testMismatchingCombiner() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      GeneratedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_COMBINER_CLASS,
                      GeneratedVertexMismatchCombiner.class,
                      VertexCombiner.class);
        try {
            mapper.determineClassTypes(conf);
            throw new RuntimeException(
                "testMismatchingCombiner: Should have caught an exception!");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testJsonBase64FormatType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        GraphMapper<?, ?, ?, ?> mapper = new GraphMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      GeneratedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      JsonBase64VertexInputFormat.class,
                      VertexInputFormat.class);
        conf.setClass(GiraphJob.VERTEX_OUTPUT_FORMAT_CLASS,
                      JsonBase64VertexOutputFormat.class,
                      VertexOutputFormat.class);
        mapper.determineClassTypes(conf);
    }
}
