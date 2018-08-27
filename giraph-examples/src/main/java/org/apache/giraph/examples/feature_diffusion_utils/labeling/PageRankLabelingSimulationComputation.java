package org.apache.giraph.examples.feature_diffusion_utils.labeling;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.LabelingVertexValue;

public class PageRankLabelingSimulationComputation extends BasicComputation<LongWritable,LabelingVertexValue, NullWritable, Text> {


	Logger LOG = Logger.getLogger(this.getClass());

	@Override
	public void compute(Vertex<LongWritable, LabelingVertexValue, NullWritable> vertex, Iterable<Text> msgs)
			throws IOException {		
		//delta=Double.parseDouble(getConf().getStrings("Delta", "0.005")[0]);
		LabelingVertexValue value = vertex.getValue();
		if (getSuperstep() >= 1) {
		      double sum = 0;
		      for (Text message : msgs) {
		        sum += Double.parseDouble(message.toString());
		      }
		      double pr=((0.15f / getTotalNumVertices()) + 0.85f * sum);
		      value.setTemp(pr);//to change, removing inty
		    
		     System.out.println("SIamo al SS "+getSuperstep()+" sono il vertice "+vertex.getId()+" e ho pr "+pr);
		      
		      
		}
		if (getSuperstep() < 50) {
			sendMessageToAllEdges(vertex, new Text(	""+ (value.getTemp() / vertex.getNumEdges())	)	);
		} else {
			int cif= (int)(Math.log10(getTotalNumVertices())+2);
			value.setLabel((long)(value.getTemp()*Math.pow(10, cif)));
			vertex.voteToHalt();
		}
	}

}