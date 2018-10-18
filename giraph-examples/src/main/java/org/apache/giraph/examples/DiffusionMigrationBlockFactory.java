package org.apache.giraph.examples;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.migration.MigrationFullBlockFactory;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.giraph.examples.feature_diffusion_utils.datastructures.DiffusionVertexValue;


public class DiffusionMigrationBlockFactory extends MigrationFullBlockFactory {

	public Block createBlock(GiraphConfiguration conf) {
		return createMigrationAppBlock(
		        DiffusionMigrationSimulationComputation.class,
		        new DiffusionMigrationMasterCompute(),
		        IntWritable.class,
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
	protected Class<DiffusionVertexValue> getVertexValueClass(GiraphConfiguration arg0) {
		return DiffusionVertexValue.class;
	}

}
