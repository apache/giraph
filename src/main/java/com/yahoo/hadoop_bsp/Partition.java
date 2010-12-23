package com.yahoo.hadoop_bsp;

import org.apache.hadoop.io.WritableComparable;

/**
 * Defines a vertex index range and assigns responsibility to a particular
 * host and port.
 * @author aching
 *
 * @param <I> vertex index type
 */
public class Partition<I extends WritableComparable>
    implements Comparable<Partition<I>> {
    /** Host that is responsible for this partition */
    private String m_hostname;
    /** Port that the host is using */
    private int m_port;
    /** Maximum vertex index of this partition */
    private I m_maxIndex;

    Partition(String hostname, int port, I maxIndex) {
        m_hostname = new String(hostname);
        m_port = port;
        m_maxIndex = maxIndex;
    }

    public String getHostname() {
        return new String(m_hostname);
    }

    public int getPort() {
        return m_port;
    }

    public I getMaxIndex() {
        return m_maxIndex;
    }

    public void setMaxIndex(I index) {
        m_maxIndex = index;
    }

  public int compareTo(Partition<I> otherObject) {
    @SuppressWarnings("unchecked")
    int compareTo =
            m_maxIndex.compareTo(((Partition<I>) otherObject).getMaxIndex());
    return compareTo;
  }
}
