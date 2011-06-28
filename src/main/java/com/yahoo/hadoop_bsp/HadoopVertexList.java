package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wrapper around {@link ArrayListWritable} that allows the hadoop vertex
 * class to be set prior to calling the default constructor.
 *
 * @param <H> Hadoop Vertex type
 */
@SuppressWarnings("rawtypes")
public class HadoopVertexList<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends ArrayListWritable<HadoopVertex<I, V, E, M>> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 1000L;

    /**
     * Default constructor for reflection
     */
    public HadoopVertexList() {}

    @Override
    public void setClass() {
        @SuppressWarnings({ "unchecked" })
        Class<HadoopVertex<I, V, E, M>> hadoopVertexClass =
            (Class<HadoopVertex<I, V, E, M>>)
                getConf().getClass(BspJob.VERTEX_CLASS,
                                   HadoopVertex.class,
                                   HadoopVertex.class);
        try {
            setClass(hadoopVertexClass);
        } catch (Exception e) {
            throw new RuntimeException(
                "setClass: Couldn't set hadoop vertex class type");
        }
    }
}
