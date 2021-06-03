from org.apache.giraph.jython import JythonComputation
from org.apache.hadoop.io import IntWritable

class CountEdges(JythonComputation):
  def compute(self, vertex, messages):
    num_edges = vertex.getNumEdges()
    vertex.setValue(IntWritable(num_edges))
    vertex.voteToHalt()
