/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class RPCCommunicationsTest {

    @Test
    public void testDuplicateRpcPort() throws Exception {
        @SuppressWarnings("rawtypes")
        Context context = mock(Context.class);
        Configuration conf = new Configuration();
        conf.setInt("mapred.task.partition", 9);
        conf.setInt(GiraphJob.MAX_WORKERS, 13);
        when(context.getConfiguration()).thenReturn(conf);
        when(context.getJobID()).thenReturn(new JobID());

        RPCCommunications<IntWritable, IntWritable, IntWritable, IntWritable>
            comm1 =
                new RPCCommunications<
                    IntWritable, IntWritable,
                    IntWritable, IntWritable>(context, null, null);
        RPCCommunications<IntWritable, IntWritable, IntWritable, IntWritable>
            comm2 =
                new RPCCommunications<
                    IntWritable, IntWritable,
                    IntWritable, IntWritable>(context, null, null);
        RPCCommunications<IntWritable, IntWritable, IntWritable, IntWritable>
            comm3 =
                new RPCCommunications<
                    IntWritable, IntWritable,
                    IntWritable, IntWritable>(context, null, null);
        assertEquals(comm1.getPort(), 30009);
        assertEquals(comm2.getPort(), 30109);
        assertEquals(comm3.getPort(), 30209);
    }
}
