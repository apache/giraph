package org.apache.giraph.examples.feature_diffusion_utils.labeling;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.migration.MigrationAbstractComputation.MigrationFullBasicComputation;
import org.apache.giraph.block_app.migration.MigrationMasterCompute.MigrationFullMasterCompute;
import org.apache.giraph.block_app.migration.MigrationFullBlockFactory;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.LabelingVertexValue;

public class LabelingMigrationBlockFactory extends MigrationFullBlockFactory{

	public Block createBlock(GiraphConfiguration conf) {
		@SuppressWarnings("unchecked")
		Class<MigrationFullBasicComputation<LongWritable,LabelingVertexValue, NullWritable, Text>> computationClass=(Class<MigrationFullBasicComputation<LongWritable,LabelingVertexValue, NullWritable, Text>>)conf.getClass("giraph.typesHolder", KCoreLabelingMigrationSimulationComputation.class);
		return createMigrationAppBlock(
				computationClass,
		        new MigrationFullMasterCompute(),
		        Text.class,
		        null,
		        conf);
	}

	@Override
	protected Class<NullWritable> getEdgeValueClass(GiraphConfiguration arg0) {
		return NullWritable.class;
	}

	@Override
	protected Class<LongWritable> getVertexIDClass(GiraphConfiguration arg0) {
		return LongWritable.class;
	}

	@Override
	protected Class<LabelingVertexValue> getVertexValueClass(GiraphConfiguration arg0) {
		return LabelingVertexValue.class;
	}
	
}
