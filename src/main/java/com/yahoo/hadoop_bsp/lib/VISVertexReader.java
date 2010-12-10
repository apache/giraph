package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.BspInputSplit;
import com.yahoo.hadoop_bsp.VertexReader;
import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * Used by VISVertexInputFormat to read VIS graph
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public class VISVertexReader extends LineRecordReader implements 
	VertexReader<Text, DoubleWritable, Float> {
	/** Logger */
    private static final Logger LOG = Logger.getLogger(VISVertexReader.class);
    /** Records read so far */
	long m_recordsRead = 0;
	
	public boolean next(Text vertexId, 
						DoubleWritable vertexValue,
						Map<Text, Float> destVertexIdEdgeValueMap) 
	    throws IOException {
           
      if (nextKeyValue() == false) {
        return false;
		  }
      Text val = getCurrentValue();
      String[] s = val.toString().split("\t");
      vertexId.set(s[0]);
      vertexValue.set(0.0);
      for (int i=1; i < s.length; i++) {
        destVertexIdEdgeValueMap.put(new Text(s[i]), 1.0f);
      }
		  ++m_recordsRead;
		  LOG.debug("next: Return vertexId=" + vertexId + ", vertexValue=" + 
				 vertexValue + ", destVertexIdEdgeValueSet=" + 
				 destVertexIdEdgeValueMap.toString());
		
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
