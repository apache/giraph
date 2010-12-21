package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.MutableVertex;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * Used by VISVertexInputFormat to read VIS graph
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public abstract class VISVertexReader<V, E>  extends LineRecordReader implements 
	VertexReader<Text, V, E>, Configurable {
	/** Logger */
    private static final Logger LOG = Logger.getLogger(VISVertexReader.class);
    /** Hadoop configuration */
    private Configuration conf;
    /** Records read so far */
	private long m_recordsRead = 0;
    /** Records skipped so far */
	private long m_skipped = 0;
    /** Input still in rdf format, has not been prepared and filtered */
    private boolean isRdf = false;
    /** Array of predicates to filter on */
    private String[] predicates = null;
	
    public void setConf(Configuration conf) {
      this.conf = conf;
      isRdf = conf.getBoolean("bsp.vis.rdf", false);
      if (isRdf) {
        String predicateFilter = conf.get("bsp.vis.filter", "");
        if (predicateFilter.equals("") == false ) {
          predicates = predicateFilter.split("[ \t,]");
          for (int i = 0; i < predicates.length; i++) {
            LOG.info("setConf: Added filter predicate=" + predicates[i]); 
          }
        }
      }
    }

    public Configuration getConf() {
      return conf;
    }

    public boolean next(
      MutableVertex<Text, V, E, ?> vertex)  
      throws IOException {
      if (!isRdf) {
        if (nextKeyValue() == false) {
          return false;
        }
	    ++m_recordsRead;
        Text val = getCurrentValue();
        String[] s = val.toString().split("\t");
        vertex.setVertexId(new Text(s[0]));
        for (int i=1; i < s.length; i++) {
          vertex.addEdge(new Text(s[i]), createEdgeValue());
        }
      } else { // unfiltered and unprepared, RDF format:
               //(subject id and name, target id and name, predicate, and more
        Text val = getCurrentValue();
        boolean sameVertex = true;
        while (sameVertex) {
          if (val == null) {
            if (nextKeyValue() == false) {
              if (vertex.getVertexId() != null) {
                return true;
              }
              return false;
            }
	        ++m_recordsRead;
	        val = getCurrentValue();
          }
          boolean skip = true;
          while (skip) {
            String[] s = val.toString().split("\t");
            if (vertex.getVertexId() != null &&
                    vertex.getVertexId().toString().equals(s[0]) == false) {
              sameVertex = false;
              break;
            }
            if (predicates != null && predicates.length > 0) {
              String predicate = s[4]; // predicate is the 4th field
              for (int i = 0; i < predicates.length; i++) {
                if (predicates[i].length() > 0 && predicate.contains(predicates[i])) {
                  skip = false;
                  break;
                }
              }
              if (skip) {
		        ++m_skipped;
                if (nextKeyValue() == false) {
                  if (vertex.getVertexId() == null) {
                    return false;
                  }
                  sameVertex = false;
		        } else {
		          ++m_recordsRead;
	              val = getCurrentValue();
                  continue;
                }
              }
            }
            skip = false;
            vertex.setVertexId(new Text(s[0]));
            vertex.addEdge(new Text(s[2]), createEdgeValue()); // single edge
            val = null;
          }
        }
      }
      if (m_recordsRead % 1000 == 0) {
		  LOG.info("next: recordsRead=" + m_recordsRead + " skipped=" + m_skipped +
                 " vertexId=" + vertex.getVertexId());
      }
		
		  return true;
	}

	public long getPos() throws IOException {
		return m_recordsRead;
	}

	public Text createVertexId() {
		return new Text();
	}

}
