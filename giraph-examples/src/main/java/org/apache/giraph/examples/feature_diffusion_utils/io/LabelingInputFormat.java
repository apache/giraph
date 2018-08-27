package org.apache.giraph.examples.feature_diffusion_utils.io;

import com.google.common.collect.Lists;

import datastructures.LabelingVertexValue;

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

public class LabelingInputFormat extends TextVertexInputFormat<LongWritable, LabelingVertexValue, NullWritable> {

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
        protected LabelingVertexValue getValue(Text line) throws IOException {
            String[] split = line.toString().split("\t");
            if (split.length==2){
                return new LabelingVertexValue();
            }else {
            	String treshold = split[split.length-2];
                return new LabelingVertexValue(Integer.parseInt(treshold));
            }

        }

    }

}