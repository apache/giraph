package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Wrapper around {@link ArrayListWritable} that allows the message class to
 * be set prior to calling readFields().
 *
 * @param <M> message type
 */
public class MsgList<M extends Writable>
    extends ArrayListWritable<M> {
    /** Defining a layout version for a serializable class. */
    private static final long serialVersionUID = 100L;

    public MsgList() {
        super();
    }

    @Override
    public void setClass() {
        @SuppressWarnings({ "unchecked" })
        Class<? extends HadoopVertex<?, ?, ?, M>> hadoopVertexClass =
            (Class<? extends HadoopVertex<?, ?, ?, M>>)
                getConf().getClass(BspJob.BSP_VERTEX_CLASS,
                                   HadoopVertex.class,
                                   HadoopVertex.class);
        try {
            @SuppressWarnings("unchecked")
            Class<M> refClass = (Class<M>)ReflectionUtils.newInstance(
                    hadoopVertexClass, getConf()).createMsgValue().getClass();
            setClass(refClass);
        } catch (Exception e) {
            throw new RuntimeException(
                "setClas: Couldn't set msg value class type");
        }
    }
}
