package com.yahoo.hadoop_bsp.lib;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.yahoo.ccm.CCMObject;
import com.yahoo.ccm.Facet;
import com.yahoo.ccm.serialization.json.CCMObjectJSONSerializer;
import com.yahoo.ccm.util.Type3UUIDGenerator;

import com.yahoo.hadoop_bsp.OutEdgeIterator;
import com.yahoo.hadoop_bsp.VertexWriter;

/**
 * Writes out VIS graph in CCM format.
 *
 */
public class VISCCMVertexWriter extends TextOutputFormat<NullWritable, Text> implements 
	VertexWriter<Text, DoubleWritable, Float> {
	/** Logger */
    private static final Logger LOG = Logger.getLogger(VISCCMVertexWriter.class);
    private final String facetName = "pop_rank_graph_feature";
    private final UUID ccmContext = Type3UUIDGenerator.generateNameBasedUUIDFromURL("vis_graph");
    private final UUID ccmWriter = Type3UUIDGenerator.generateNameBasedUUIDFromURL("ckunz");
    private final CCMObjectJSONSerializer serializer = new CCMObjectJSONSerializer();
	
	public <KEYOUT, VALUEOUT> void write(
            TaskInputOutputContext<Object, Object,
                                   KEYOUT, VALUEOUT> context,
            Text vertexId, 
			      DoubleWritable vertexValue,
			      OutEdgeIterator<Text, Float> destEdgeIt) 
	          throws IOException, InterruptedException {
           
      try {
        CCMObject obj = new CCMObject(UUID.randomUUID()); // should be the UUID of the vertex
        Facet newFacet = obj.addFacet(facetName, ccmContext, ccmWriter);

        newFacet.append("user_id", vertexId.toString()); // should be a UUID
        newFacet.append("pop_rank", vertexValue.toString());
        context.write((KEYOUT)new Text(serializer.serialize(obj)), (VALUEOUT)null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
	}

  public void close(TaskAttemptContext context
                      ) throws IOException, InterruptedException {
  }
}
