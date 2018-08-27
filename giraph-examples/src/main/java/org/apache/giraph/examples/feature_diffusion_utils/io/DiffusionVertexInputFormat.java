package org.apache.giraph.examples.feature_diffusion_utils.io;

import com.google.common.collect.Lists;

import datastructures.DiffusionVertexValue;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class DiffusionVertexInputFormat extends TextVertexInputFormat<LongWritable, DiffusionVertexValue, NullWritable> {

	@Override
	public TextVertexReader createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new DiffusionVertexReader();
	}
	
	protected class DiffusionVertexReader extends TextVertexReaderFromEachLine{

		@Override
		protected Iterable<Edge<LongWritable, NullWritable>> getEdges(Text line) throws IOException {
			String[] fA = line.toString().split("\t");
			String[] edgeArray = fA[fA.length-1].split(",");
			List<Edge<LongWritable, NullWritable>> edges =	Lists.newArrayList();
			int i;
			for (i = 0; i < edgeArray.length; ++i) {
				long neighborId = Long.parseLong(edgeArray[i]);
				edges.add(EdgeFactory.create(new LongWritable(neighborId),
						NullWritable.get()));
			}
			return edges;
		}

		@Override
		protected LongWritable getId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split("\t")[0]));
		}

		@Override
		protected DiffusionVertexValue getValue(Text line) throws IOException {
			String[] split = line.toString().split("\t");
			String value=split[1];
			String [] reSplit=value.split(",");
			if(reSplit.length==2) {
				int treshold= Integer.parseInt(reSplit[1]);
				int label=Integer.parseInt(reSplit[0]);
				return new DiffusionVertexValue(treshold,label);
				
			}else {
				int label=Integer.parseInt(reSplit[0]);
				return new DiffusionVertexValue(label);
				
			}
		}
	}

}
