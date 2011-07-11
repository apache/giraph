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
import org.apache.giraph.graph.GiraphJob.BspMapper;
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

    public void testMatchingType() throws SecurityException, NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        BspMapper<?, ?, ?, ?> mapper = new BspMapper();
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
        mapper.checkClassTypes(conf);
    }

    public void testDerivedMatchingType() throws SecurityException, NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        BspMapper<?, ?, ?, ?> mapper = new BspMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      DerivedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        mapper.checkClassTypes(conf);
    }

    public void testDerivedInputFormatType() throws SecurityException, NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        BspMapper<?, ?, ?, ?> mapper = new BspMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      DerivedVertexMatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        mapper.checkClassTypes(conf);
    }

    public void testMismatchingVertex() throws SecurityException, NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        BspMapper<?, ?, ?, ?> mapper = new BspMapper();
        Configuration conf = new Configuration();
        conf.setClass(GiraphJob.VERTEX_CLASS,
                      GeneratedVertexMismatch.class,
                      Vertex.class);
        conf.setClass(GiraphJob.VERTEX_INPUT_FORMAT_CLASS,
                      SimpleSuperstepVertexInputFormat.class,
                      VertexInputFormat.class);
        try {
            mapper.checkClassTypes(conf);
            throw new RuntimeException(
                "testMismatchingVertex: Should have caught an exception!");
        } catch (IllegalArgumentException e) {
        }
    }

    public void testMismatchingCombiner() throws SecurityException, NoSuchMethodException, NoSuchFieldException {
        @SuppressWarnings("rawtypes")
        BspMapper<?, ?, ?, ?> mapper = new BspMapper();
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
            mapper.checkClassTypes(conf);
            throw new RuntimeException(
                "testMismatchingCombiner: Should have caught an exception!");
        } catch (IllegalArgumentException e) {
        }
    }
}
