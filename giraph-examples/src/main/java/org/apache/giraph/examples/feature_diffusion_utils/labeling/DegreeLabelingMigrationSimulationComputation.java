package org.apache.giraph.examples.feature_diffusion_utils.labeling;

import java.io.IOException;

import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullBasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.LabelingVertexValue;

public class DegreeLabelingMigrationSimulationComputation extends MigrationFullBasicComputation<LongWritable,LabelingVertexValue, NullWritable, Text> {


	Logger LOG = Logger.getLogger(this.getClass());


	/*public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<LongWritable, DiffusionVertexValue, NullWritable> workerClientRequestProcessor,
			CentralizedServiceWorker<LongWritable, DiffusionVertexValue, NullWritable> serviceWorker,
			WorkerGlobalCommUsage workerGlobalCommUsage) {
		super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
		delta = getConf().getDouble(DiffusionMasterCompute.diffusionDeltaOption, DiffusionMasterCompute.diffusionDeltaOptionDefault);
		modelSwitch = getConf().getBoolean(DiffusionMasterCompute.diffusionListenOption, false);

	}*/

	@Override
	public void compute(Vertex<LongWritable, LabelingVertexValue, NullWritable> vertex, Iterable<Text> msgs)
			throws IOException {		
		LabelingVertexValue value = vertex.getValue();
		value.setLabel(vertex.getNumEdges());
		vertex.voteToHalt();
	}
	
}
