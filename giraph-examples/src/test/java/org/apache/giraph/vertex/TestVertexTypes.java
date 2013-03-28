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

package org.apache.giraph.vertex;

import org.apache.giraph.combiner.Combiner;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexValueFactory;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.io.formats.JsonBase64VertexInputFormat;
import org.apache.giraph.io.formats.JsonBase64VertexOutputFormat;
import org.apache.giraph.job.GiraphConfigurationValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_VALUE_FACTORY_CLASS;


public class TestVertexTypes {

    /**
     * Matches the {@link GeneratedVertexInputFormat}
     */
    private static class GeneratedVertexMatch extends Vertex<LongWritable,
        IntWritable, FloatWritable, FloatWritable> {
        @Override
        public void compute(Iterable<FloatWritable> messages)
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
    private static class GeneratedVertexMismatch extends Vertex<LongWritable,
        FloatWritable, FloatWritable, FloatWritable> {
        @Override
        public void compute(Iterable<FloatWritable> messages)
                throws IOException {
        }
    }

    /**
     * Matches the {@link GeneratedVertexMatch}
     */
    private static class GeneratedVertexMatchCombiner extends
        Combiner<LongWritable, FloatWritable> {
      @Override
      public void combine(LongWritable vertexIndex,
          FloatWritable originalMessage,
          FloatWritable messageToCombine) {
      }

      @Override
      public FloatWritable createInitialMessage() {
        return null;
      }
    }

    /**
     * Mismatches the {@link GeneratedVertexMatch}
     */
    private static class GeneratedVertexMismatchCombiner extends
        Combiner<LongWritable, DoubleWritable> {
      @Override
      public void combine(LongWritable vertexIndex,
          DoubleWritable originalMessage,
          DoubleWritable messageToCombine) {
      }

      @Override
      public DoubleWritable createInitialMessage() {
        return null;
      }
    }

    /**
     * Mismatches the {@link GeneratedVertexMatch}
     */
    private static class GeneratedVertexMismatchValueFactory implements
        VertexValueFactory<DoubleWritable> {

      @Override
      public void initialize(
          ImmutableClassesGiraphConfiguration<?, DoubleWritable, ?, ?>
              configuration) {}

      @Override
      public DoubleWritable createVertexValue() {
        return new DoubleWritable();
      }
    }

    /**
     * Just populate a conf with testing defaults that won't
     * upset the GiraphConfigurationValidator.
     * */
    private Configuration getDefaultTestConf() {
      Configuration conf = new Configuration();
      conf.setInt(GiraphConstants.MAX_WORKERS, 1);
      conf.setInt(GiraphConstants.MIN_WORKERS, 1);
      conf.set(GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.getKey(),
        "org.apache.giraph.io.formats.DUMMY_TEST_VALUE");
      return conf;
    }

    @Test
    public void testMatchingType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        Configuration conf = getDefaultTestConf();
        GiraphConstants.VERTEX_CLASS.set(conf, GeneratedVertexMatch.class);
        GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
        GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
            SimpleSuperstepVertexInputFormat.class);
        GiraphConstants.VERTEX_COMBINER_CLASS.set(conf,
            GeneratedVertexMatchCombiner.class);
      @SuppressWarnings("rawtypes")
      GiraphConfigurationValidator<?, ?, ?, ?> validator =
        new GiraphConfigurationValidator(conf);

      ImmutableClassesGiraphConfiguration gc = new
          ImmutableClassesGiraphConfiguration(conf);


      validator.validateConfiguration();
    }

    @Test
    public void testDerivedMatchingType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        Configuration conf = getDefaultTestConf() ;
        GiraphConstants.VERTEX_CLASS.set(conf, DerivedVertexMatch.class);
        GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
        GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
            SimpleSuperstepVertexInputFormat.class);
        @SuppressWarnings("rawtypes")
        GiraphConfigurationValidator<?, ?, ?, ?> validator =
          new GiraphConfigurationValidator(conf);
        validator.validateConfiguration();
    }

    @Test
    public void testDerivedInputFormatType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        Configuration conf = getDefaultTestConf() ;
        GiraphConstants.VERTEX_CLASS.set(conf, DerivedVertexMatch.class);
        GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
        GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
            SimpleSuperstepVertexInputFormat.class);
      @SuppressWarnings("rawtypes")
      GiraphConfigurationValidator<?, ?, ?, ?> validator =
        new GiraphConfigurationValidator(conf);
      validator.validateConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchingVertex() throws SecurityException,
      NoSuchMethodException, NoSuchFieldException {
      Configuration conf = getDefaultTestConf() ;
      GiraphConstants.VERTEX_CLASS.set(conf, GeneratedVertexMismatch.class);
      GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
        SimpleSuperstepVertexInputFormat.class);
      @SuppressWarnings("rawtypes")
      GiraphConfigurationValidator<?, ?, ?, ?> validator =
        new GiraphConfigurationValidator(conf);
      validator.validateConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchingCombiner() throws SecurityException,
      NoSuchMethodException, NoSuchFieldException {
      Configuration conf = getDefaultTestConf() ;
      GiraphConstants.VERTEX_CLASS.set(conf, GeneratedVertexMatch.class);
      GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
        SimpleSuperstepVertexInputFormat.class);
      GiraphConstants.VERTEX_COMBINER_CLASS.set(conf,
        GeneratedVertexMismatchCombiner.class);
      @SuppressWarnings("rawtypes")
      GiraphConfigurationValidator<?, ?, ?, ?> validator =
        new GiraphConfigurationValidator(conf);
      validator.validateConfiguration();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchingVertexValueFactory() throws SecurityException,
        NoSuchMethodException, NoSuchFieldException {
      Configuration conf = getDefaultTestConf() ;
      GiraphConstants.VERTEX_CLASS.set(conf, GeneratedVertexMatch.class);
      GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
      GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
          SimpleSuperstepVertexInputFormat.class);
      VERTEX_VALUE_FACTORY_CLASS.set(conf,
          GeneratedVertexMismatchValueFactory.class);
      @SuppressWarnings("rawtypes")
      GiraphConfigurationValidator<?, ?, ?, ?> validator =
          new GiraphConfigurationValidator(conf);
      validator.validateConfiguration();
    }

    @Test
    public void testJsonBase64FormatType() throws SecurityException,
            NoSuchMethodException, NoSuchFieldException {
        Configuration conf = getDefaultTestConf() ;
        GiraphConstants.VERTEX_CLASS.set(conf, GeneratedVertexMatch.class);
        GiraphConstants.VERTEX_EDGES_CLASS.set(conf, ByteArrayEdges.class);
        GiraphConstants.VERTEX_INPUT_FORMAT_CLASS.set(conf,
            JsonBase64VertexInputFormat.class);
        GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS.set(conf,
            JsonBase64VertexOutputFormat.class);
        @SuppressWarnings("rawtypes")
        GiraphConfigurationValidator<?, ?, ?, ?> validator =
          new GiraphConfigurationValidator(conf);
        validator.validateConfiguration();
    }
}
