package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullBasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.DiffusionVertexValue;

public class DiffusionMigrationSimulationComputation extends MigrationFullBasicComputation<LongWritable, DiffusionVertexValue, NullWritable, IntWritable> {

	Logger LOG = Logger.getLogger(this.getClass());



	/*public void initialize(GraphState graphState,
			WorkerClientRequestProcessor<LongWritable, DiffusionVertexValue, NullWritable> workerClientRequestProcessor,
			CentralizedServiceWorker<LongWritable, DiffusionVertexValue, NullWritable> serviceWorker,
			WorkerGlobalCommUsage workerGlobalCommUsage) {
		super.initialize(graphState, workerClientRequestProcessor, serviceWorker, workerGlobalCommUsage);
		delta = getConf().getDouble(DiffusionMigrationMasterCompute.diffusionDeltaOption, DiffusionMigrationMasterCompute.diffusionDeltaOptionDefault);
		modelSwitch = getConf().getBoolean(DiffusionMigrationMasterCompute.diffusionListenOption, false);

	}*/

	@Override
	public void compute(Vertex<LongWritable, DiffusionVertexValue, NullWritable> vertex, Iterable<IntWritable> msgs)
			throws IOException {		
		
		DiffusionVertexValue value = vertex.getValue();
		if(getSuperstep()==0) { //First superstep just to know the first label to analyze
			setup(value);
		}else {
			boolean  byLabel = ((BooleanWritable)getAggregatedValue(DiffusionMigrationMasterCompute.byLabelOption)).get();
			int currentLabel = (int)((LongWritable)getAggregatedValue(DiffusionMigrationMasterCompute.currentLabel)).get();
			// aggregators must be analyzed after first superstep
			boolean timeToSwitch = ((BooleanWritable)getAggregatedValue(DiffusionMigrationMasterCompute.timeToSwitch)).get();
			if (timeToSwitch)//time to switch label?
				if(value.getLabel()<currentLabel)
					aggregate(DiffusionMigrationMasterCompute.nextLabel, new LongWritable(value.getLabel()));
			if(value.isVertexInvited(currentLabel)) {
				if (byLabel) {
					if(!value.isVertexDead()) {		//Update the using probability, if not dead
						int activeNeighbors = checkMsgsAndUpdateProbability(msgs, value);					
						if(activeNeighbors==value.getVertexThreshold())
							aggregate(DiffusionMigrationMasterCompute.hesitantVerticesAggregator,new LongWritable(1));
					}
					aggregateVerticesBasedOnProbability(value);
					if(value.rollActivationDice()) { //if this vertex is using the feature
						aggregate(DiffusionMigrationMasterCompute.usingVerticesAggregator, new LongWritable(1));
						sendMessageToAllEdges(vertex, new IntWritable(1));
					}	
				}else {//computation by min number
					if(!timeToSwitch) {				
						boolean justChanged = ((BooleanWritable)getAggregatedValue(DiffusionMigrationMasterCompute.justChangedTimeToSwitch)).get();
						//Update the using probability if not dead and the computation has not just became active 
						//(because we don't have old messages sent so it would wrongly decrease the probability)
						if(!value.isVertexDead() && !justChanged) {
							int activeNeighbors = checkMsgsAndUpdateProbability(msgs, value);					
							if(activeNeighbors==value.getVertexThreshold())
								aggregate(DiffusionMigrationMasterCompute.hesitantVerticesAggregator,new LongWritable(1));
						}
						aggregateVerticesBasedOnProbability(value);
						if(value.rollActivationDice()) { //if this vertex is using the feature
							aggregate(DiffusionMigrationMasterCompute.usingVerticesAggregator, new LongWritable(1));
						sendMessageToAllEdges(vertex, new IntWritable(1));
						}
					}else{ //if computation is not active and we're using a min number switch model
						aggregate(DiffusionMigrationMasterCompute.potentialVerticesAggregator, new LongWritable(1));
					}
				}
			}
			//vertex.voteToHalt();
		}
		
		
	}
	
	private void setup(DiffusionVertexValue value) {
		double delta = Double.parseDouble(getConf().getStrings("Delta", "0.005")[0]);
		value.setDelta(delta);
		double initialActivationProbability = Double.parseDouble(getConf().getStrings("InitialProbability","0.02")[0]);
		value.setInitialActivationProbability(initialActivationProbability);
		double almostConvincedTreshold = Double.parseDouble(getConf().getStrings("AlmostConvincedTreshold","0.7")[0]);
		value.setAlmostConvincedTreshold(almostConvincedTreshold);
		aggregate(DiffusionMigrationMasterCompute.nextLabel, new LongWritable(value.getLabel()));
		
		String thresholdType = getConf().getStrings("ThresholdType", "")[0];
		if (thresholdType.compareTo("1")==0)
			value.setVertexThreshold(1);
		else if (thresholdType.compareTo("Prop")==0) {
			value.setVertexThreshold((int)value.getLabel()/20);
		}
	}
	
	private int checkMsgsAndUpdateProbability(Iterable<IntWritable> msgs, DiffusionVertexValue value) {
		Iterator<IntWritable> it = msgs.iterator();
		int activeNeighbors = 0;
		while(it.hasNext())
			activeNeighbors += it.next().get();						
		if(activeNeighbors > value.getVertexThreshold())
			value.modifyCurrentActivationProbability(1);
		else if(activeNeighbors < value.getVertexThreshold())
			value.modifyCurrentActivationProbability(-1);
		return activeNeighbors;
	}
	
	private void aggregateVerticesBasedOnProbability(DiffusionVertexValue value) {
		if(value.isVertexConvinced())
			aggregate(DiffusionMigrationMasterCompute.convincedVerticesAggregator, new LongWritable(1));
		//LOG.info("I'm with a probability " + value.getCurrentActivationProbability());
		if(value.isVertexDead()) { //Dead aggregator update
			aggregate(DiffusionMigrationMasterCompute.deadVerticesAggregator, new LongWritable(1));
		}
		if(value.isAlmostConvinced()){
			aggregate(DiffusionMigrationMasterCompute.almostConvincedVerticesAggregator,new LongWritable(1));
		}
		aggregate(DiffusionMigrationMasterCompute.invitedVerticesAggregator, new LongWritable(1));

	}
	
}