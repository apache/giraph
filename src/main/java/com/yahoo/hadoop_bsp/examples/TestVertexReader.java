package com.yahoo.hadoop_bsp.examples;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.yahoo.hadoop_bsp.BspInputSplit;
import com.yahoo.hadoop_bsp.VertexReader;
import com.yahoo.hadoop_bsp.BspJob.BspMapper;

/**
 * Used by TestVertexInputFormat to read some generated data
 * @author aching
 *
 * @param <I>
 * @param <V>
 * @param <E>
 */
public class TestVertexReader implements VertexReader<Long, Integer, Float> {
	/** Logger */
    private static final Logger LOG = Logger.getLogger(BspMapper.class);
    /** Records read so far */
	long m_recordsRead = 0;
	/** Total records to read */
	long m_totalRecords = 0;
	/** The input split from initialize(). */
	BspInputSplit m_inputSplit = null;
	
	public static final String READER_VERTICES = 
		"TestVertexReader.reader_vertices";
	public static final long DEFAULT_READER_VERTICES = 10;
	
	public void initialize(
		InputSplit inputSplit, TaskAttemptContext context)
		throws IOException {
		Configuration configuration = context.getConfiguration();
			m_totalRecords = configuration.getLong(
				TestVertexReader.READER_VERTICES,
				TestVertexReader.DEFAULT_READER_VERTICES);
			m_inputSplit = (BspInputSplit) inputSplit;
	}
	
	public boolean next(Long vertexId, 
						Integer vertexValue,
						Set<Float> edgeValueSet) throws IOException {
		if (m_totalRecords <= m_recordsRead) {
			return false;
		}
		vertexId = 
			(m_inputSplit.getNumSplits() * m_totalRecords) + m_recordsRead;
		vertexValue = (int) (vertexId * 10);
		edgeValueSet.add((float) vertexId * 100);
		++m_recordsRead;
		LOG.info("next: Return vertexId=" + vertexId + ", vertexValue=" + 
				 vertexValue + ", edge=" + edgeValueSet);
		
		return true;
	}

	public long getPos() throws IOException {
		return m_recordsRead;
	}

	public void close() throws IOException {
	}

	public float getProgress() throws IOException {
		return m_recordsRead * 100.0f / m_totalRecords;
	}

	public Long createVertexId() {
		return new Long(-1);
	}

	public Integer createVertexValue() {
		return new Integer(-1);
	}

	public Float createEdgeValue() {
		return new Float(0.0);
	}
}
