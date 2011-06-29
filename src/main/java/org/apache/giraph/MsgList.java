package org.apache.giraph;

import org.apache.hadoop.io.Writable;

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

    @SuppressWarnings("unchecked")
    @Override
    public void setClass() {
        setClass((Class<M>) BspUtils.getMessageValueClass(getConf()));
    }
}
