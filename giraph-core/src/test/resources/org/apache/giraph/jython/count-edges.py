from org.apache.giraph.graph import BasicComputation
from org.apache.hadoop.io import IntWritable

class CountEdges(BasicComputation):
  def compute(self, vertex, messages):
    num_edges = vertex.getNumEdges()
    vertex.setValue(IntWritable(num_edges))
    vertex.voteToHalt()
