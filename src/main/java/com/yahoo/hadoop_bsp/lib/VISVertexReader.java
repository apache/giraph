package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.VertexReader;

/**
 * Used by VISVertexInputFormat to read VIS graph
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public class VISVertexReader extends LineRecordReader implements 
	VertexReader<Text, DoubleWritable, Float>, Configurable {
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

	public boolean next(Text vertexId, 
						DoubleWritable vertexValue,
						Map<Text, Float> destVertexIdEdgeValueMap) 
	    throws IOException {
           
      if (nextKeyValue() == false) {
        return false;
		  }
		  ++m_recordsRead;
      if (!isRdf) {
      Text val = getCurrentValue();
      String[] s = val.toString().split("\t");
      vertexId.set(s[0]);
      vertexValue.set(0.0);
      for (int i=1; i < s.length; i++) {
        destVertexIdEdgeValueMap.put(new Text(s[i]), 1.0f);
      }
      } else { // unfiltered and unprepared, RDF format:
               //(subject id and name, target id and name, predicate, and more
        boolean skip = true;
        while (skip) {
          Text val = getCurrentValue();
          String[] s = val.toString().split("\t");
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
                return false;
		      }
		      ++m_recordsRead;
              continue;
            }
          }
          skip = false;
          vertexId.set(s[0]);
          vertexValue.set(0.0);
          destVertexIdEdgeValueMap.put(new Text(s[2]), 1.0f); // single edge
        }
      }
      if (m_recordsRead % 1000 == 0) {
		  LOG.info("next: recordsRead=" + m_recordsRead + " skipped=" + m_skipped +
                 " vertexId=" + vertexId + ", vertexValue=" + 
				 vertexValue + ", destVertexIdEdgeValueSet=" + 
				 destVertexIdEdgeValueMap.toString());
      }
		
		  return true;
	}

	public long getPos() throws IOException {
		return m_recordsRead;
	}

	public Text createVertexId() {
		return new Text();
	}

	public DoubleWritable createVertexValue() {
		return new DoubleWritable(0.0);
	}

	public Float createEdgeValue() {
		return new Float(0.0);
	}
}
