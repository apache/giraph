package com.yahoo.hadoop_bsp;

/**
 * Represents a range of responsibility on a process for an index range of keys
 * @author aching
 *
 * @param <I> vertex index type
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

	public void setMaxIndex(I index) {
		m_maxIndex = index;
	}

  public int compareTo(Partition<I> otherObject) {
    @SuppressWarnings("unchecked")
    Comparable<I> comparable =
                (Comparable<I>) m_maxIndex;
    return comparable.compareTo(
                  ((Partition<I>) otherObject).getMaxIndex());
  }
}
