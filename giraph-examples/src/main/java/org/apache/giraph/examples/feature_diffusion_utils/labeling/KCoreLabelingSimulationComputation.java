package org.apache.giraph.examples.feature_diffusion_utils.labeling;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.LabelingVertexValue;

@SuppressWarnings("unused")
public class KCoreLabelingSimulationComputation extends BasicComputation<LongWritable,LabelingVertexValue, NullWritable, Text> {

	Logger LOG = Logger.getLogger(this.getClass());

	@Override
	public void compute(Vertex<LongWritable, LabelingVertexValue, NullWritable> vertex, Iterable<Text> msgs)
			throws IOException {		
		//delta=Double.parseDouble(getConf().getStrings("Delta", "0.005")[0]);
		LabelingVertexValue value = vertex.getValue();
		if(getSuperstep()==0) {
			value.setLabel(Integer.max(vertex.getNumEdges(),1));
			//value.setNeighboorsLabel(vertex.getNumEdges());
			sendMessageToAllEdges(vertex, new Text(""+vertex.getId().get()+" "+value.getLabel()));
			value.setChanged(false);
		}else {
			
			for(Text msg: msgs) {
				long id = Long.parseLong(msg.toString().split(" ")[0]);
				int coreness = Integer.parseInt(msg.toString().split(" ")[1]);
				value.updateNeighboorLabel(id, coreness);
			}
			
			int tempLabel = computeIndex(value.getNeighboorsLabel(),value.getLabel());
			if (tempLabel<value.getLabel())
				value.setLabel(tempLabel);
			if(value.isChanged()) {
				sendMessageToAllEdges(vertex, new Text(""+vertex.getId().get()+" "+value.getLabel()));
				value.setChanged(false);
			}
		}
		vertex.voteToHalt();
	}


	private int computeIndex(HashMap<Long, Long> neighboorsLabel, long coreness) {
		int[] corenessCount = new int[(int) coreness];
		for (int i = 0 ; i<coreness ; i++)
			corenessCount[i]=0;
		for (Entry<Long, Long> pair: neighboorsLabel.entrySet()) {
			long corenessCandidate =Long.min( pair.getValue() , coreness);
			corenessCount[(int)corenessCandidate-1]++;
		}
		for (int i=(int) (coreness-1); i>0 ; i--)
			corenessCount[i-1]+=corenessCount[i];
		int i = (int) coreness;
		while(i>1 && corenessCount[i-1]<i) {
			i--;
		}
		return i;
	}

}
