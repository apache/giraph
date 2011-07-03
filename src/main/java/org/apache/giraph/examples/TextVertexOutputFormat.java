package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Abstract class that users should subclass to use their own text based
 * vertex output format.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class TextVertexOutputFormat<
    I extends WritableComparable, V extends Writable, E extends Writable>
    extends TextOutputFormat<BasicVertex<I, V, E, Writable>, Writable>
    implements VertexOutputFormat<I, V, E> {
}
