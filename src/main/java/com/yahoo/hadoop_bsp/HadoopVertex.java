import java.util.Iterator;
import java.util.Set;

public abstract class HadoopVertex<V, E, M> implements Vertex<V, E, M> {
	private static final int m_superstep = 0;
	private V m_vertexValue;
	private Set<E> m_outEdgeSet;
	private boolean m_halt = false;
	
	public int getSuperstep() {
		return m_superstep;
	}
	public V getVertexValue() {
		return m_vertexValue;
	}
	public void setVertexValue(V vertexValue) {
		m_vertexValue = vertexValue;
	}
	
	public Iterator getOutEdgeIterator() {
		return m_outEdgeSet.iterator();
	}
	
	public void sendMsg(String vertex, M msg) {
		
	}
	
	public void voteToHalt() {
		m_halt = true;
	}
}

