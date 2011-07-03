/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

/**
 * This InputFormat supports the BSP model by ensuring that the user specifies
 * how many splits (number of mappers) should be started simultaneously.
 * The number of splits depends on whether the master and worker processes are
 * separate.  It is not meant to do any meaningful split of user-data.
 */
public class BspInputFormat extends InputFormat<Text, Text> {
    /** Logger */
    private static final Logger LOG = Logger.getLogger(BspInputFormat.class);

    /**
     * Get the correct number of mappers based on the configuration
     *
     * @param conf Configuration to determine the number of mappers
     */
    public static int getMaxTasks(Configuration conf) {
        int maxWorkers = conf.getInt(GiraphJob.MAX_WORKERS, 0);
        boolean splitMasterWorker =
            conf.getBoolean(GiraphJob.SPLIT_MASTER_WORKER,
                            GiraphJob.SPLIT_MASTER_WORKER_DEFAULT);
        int maxTasks = maxWorkers;
        if (splitMasterWorker) {
            int zkServers =
                conf.getInt(GiraphJob.ZOOKEEPER_SERVER_COUNT,
                            GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
            maxTasks += zkServers;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("getMaxTasks: Max workers = " + maxWorkers +
                      ", split master/worker = " + splitMasterWorker +
                      ", total max tasks = " + maxTasks);
        }
        return maxTasks;
    }

    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int maxTasks = getMaxTasks(conf);
        if (maxTasks <= 0) {
            throw new InterruptedException(
                "getSplits: Cannot have maxTasks <= 0 - " + maxTasks);
        }
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < maxTasks; ++i) {
            inputSplitList.add(new BspInputSplit());
        }
        return inputSplitList;
    }

    public RecordReader<Text, Text>
        createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
        return new BspRecordReader();
    }
}
