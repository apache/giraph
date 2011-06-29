package org.apache.giraph.comm;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper around {@link ArrayListWritable} that allows the vertex
 * class to be set prior to calling the default constructor.
 *
 * @param <H> Hadoop Vertex type
 */
@SuppressWarnings("rawtypes")
public class VertexList<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends ArrayListWritable<Vertex<I, V, E, M>> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1000L;

    /**
     * Default constructor for reflection
     */
    public VertexList() {}

    @SuppressWarnings("unchecked")
    @Override
    public void setClass() {
        setClass((Class<Vertex<I, V, E, M>>)
                 BspUtils.<I, V, E, M>getVertexClass(getConf()));
    }
}
