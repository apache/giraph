package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.MutableVertex;
import com.yahoo.hadoop_bsp.VertexReader;

/**
 * Used by VISVertexInputFormat to read VIS graph
 *
 */
public class VISIntVertexReader extends LineRecordReader implements 
   VertexReader<IntWritable, DoubleWritable, Float> {
	    /** Logger */
    private static final Logger LOG = Logger.getLogger(VISVertexReader.class);
    /** Records read so far */
	long m_recordsRead = 0;
	
	public boolean next(
	    MutableVertex<IntWritable, DoubleWritable, Float, ?> vertex)  
	    throws IOException {
	    if (nextKeyValue() == false) {
	        return false;
	    }
	    Text val = getCurrentValue();
	    String[] s = val.toString().split("\t");
	    try {
	        vertex.setVertexId(new IntWritable(Integer.parseInt(s[0])));
	        vertex.setVertexValue(new DoubleWritable(0.0f));
	        for (int i=1; i < s.length; i++) {
	            vertex.addEdge(new IntWritable(Integer.parseInt(s[i])), 
	                           new Float(1.0f));
	        }
	    } catch (NumberFormatException e) {
	        throw new RuntimeException(e);
	    }
	    ++m_recordsRead;
		      LOG.debug("next: Return vertexId=" + vertex.getVertexId() + 
		                ", vertexValue=" + vertex.getVertexValue());
		
	    return true;
	}

	public long getPos() throws IOException {
		return m_recordsRead;
	}

	public IntWritable createVertexId() {
		return new IntWritable();
	}

	public DoubleWritable createVertexValue() {
		return new DoubleWritable(0.0);
	}

	public Float createEdgeValue() {
		return new Float(0.0);
	}
}
