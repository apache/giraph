# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Giraph Debugger

## Overview
Graft is a debugging and testing tool for programs written for [Apache Giraph](https://giraph.apache.org/). In particular Graft helps users find bugs in their [_Computation.compute()_](http://giraph.apache.org/apidocs/org/apache/giraph/graph/Computation.html) and [_Master.compute()_](https://giraph.apache.org/giraph-core/apidocs/org/apache/giraph/master/MasterCompute.html) methods that result in an incorrect computation being made on the graph, such as incorrect messages being sent between vertices, vertices being assigned incorrect vertex values, or aggregators being updating in an incorrect way. Graft is NOT designed for identifying performance bottlenecks in Giraph programs or Giraph itself.  For more information, visit [Graft's wiki](https://github.com/semihsalihoglu/graft/wiki).



## Quick Start

### Installing Graft

#### Get Prerequisites
The following are required to build and run Graft:

* Protocol Buffers
* JDK 7
* Maven 3
* Git

Please check the [wiki page for detailed installation instructions](https://github.com/semihsalihoglu/graft/wiki/Installing-Graft).

Make sure everything required for Giraph is also installed, such as:

* Hadoop


#### Get Giraph Trunk
Graft must be built as a module for Giraph trunk, so let's grab a copy of it:
```bash
git clone https://github.com/apache/giraph.git -b trunk
cd giraph
mvn -DskipTests --projects ./giraph-core install
```

#### Get Graft under Giraph, Build and Install It
Get a copy of Graft as giraph-debugger module in Giraph trunk:
```bash
git clone https://github.com/semihsalihoglu/graft.git  giraph-debugger
cd giraph-debugger
mvn -DskipTests compile
```

Add current directory to PATH, so we can easily run giraph-debug later:
```bash
PATH=$PWD:$PATH
```
You can add the line to your shell configuration.  For example, if you use bash:
```bash
echo PATH=$PWD:\$PATH >>~/.bash_profile
```
Now, let's debug an example Giraph job.


### Launching Giraph Jobs with Graft

#### Download a Sample Graph
Before we move on, let's download a small sample graph:
```bash
curl -L http://ece.northwestern.edu/~aching/shortestPathsInputGraph.tar.gz | tar xfz -
hadoop fs -put shortestPathsInputGraph shortestPathsInputGraph
```
You must have your system configured to use a Hadoop cluster, or run one on your local machine with the following command:
```bash
start-all.sh
```

#### Launch Giraph's Shortest Path Example
Next, let's compile the giraph-examples module:
```bash
cd ../giraph-examples
mvn -DskipTests compile
```

Here's how you would typically launch a Giraph job with GiraphRunner class (the simple shortest paths example):
```bash
hadoop jar \
    target/giraph-examples-*.jar org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.SimpleShortestPathsComputation \
    -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat \
    -vip shortestPathsInputGraph \
    -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat \
    -op shortestPathsOutputGraph.$RANDOM \
    -w 1 \
    -ca giraph.SplitMasterWorker=false \
    #
```

#### Launch It in Debugging Mode with Graft
Now, you can launch the Giraph job in debugging mode by simply replacing the first two words (`hadoop jar`) of the command with `giraph-debug`:

```bash
giraph-debug \
    target/giraph-examples-*.jar org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.SimpleShortestPathsComputation \
    # ... rest are the same as above
```
Find the job identifier from the output, e.g., `job_201405221715_0005` and copy it for later.

You can optionally specify the supersteps and vertex IDs to debug:
```bash
giraph-debug -S{0,1,2} -V{1,2,3,4,5} -S 2 \
    target/giraph-examples-*.jar org.apache.giraph.GiraphRunner \
    org.apache.giraph.examples.SimpleShortestPathsComputation \
    # ...
```

### Accessing Captured Debug Traces with Graft

#### Launch Debugger GUI
Launch the debugger GUI with the following command:
```bash
giraph-debug gui
```
Then open <http://localhost:8000> from your web browser, and paste the job ID to browse it after the job has finished.

If necessary, you can specify a different port number when you launch the GUI.
```bash
giraph-debug gui 12345
```

#### Or, Stay on the Command-line to Debug
You can access all information that has been recorded by the debugging Giraph job using the following commands.

##### List Recorded Traces
```bash
giraph-debug list
giraph-debug list job_201405221715_0005
```

##### Dump a Trace
```bash
giraph-debug dump job_201405221715_0005 0 6
```

##### Generate JUnit Test Case Code from a Trace
```bash
giraph-debug mktest        job_201405221715_0005 0 6 Test_job_201405221715_0005_S0_V6
giraph-debug mktest-master job_201405221715_0005 0   TestMaster_job_201405221715_0005_S0
```
