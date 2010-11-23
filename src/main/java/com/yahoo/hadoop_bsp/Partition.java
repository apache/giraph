package com.yahoo.hadoop_bsp;

/**
 * Represents a range of responsibility for an index range of keys
 * @author aching
 *
 * @param <I>
 */
public class Partition<I> implements Comparable<Partition<I>> {
	private String m_hostname;
	private int m_port;
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

    public int compareTo(Partition<I> otherObject) {
          return ((Comparable<I>) m_maxIndex).compareTo(
                  ((Partition<I>) otherObject).getMaxIndex());
    }
}
