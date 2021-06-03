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

import org.apache.giraph.comm.netty.NettyClient;
import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.handler.SaslServerHandler;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.IntNoOpComputation;
import org.apache.giraph.utils.MockUtils;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Netty connection with mocked authentication.
 */
public class SaslConnectionTest {
  /** Class configuration */
  private ImmutableClassesGiraphConfiguration conf;

  @Before
  public void setUp() {
    GiraphConfiguration tmpConfig = new GiraphConfiguration();
    tmpConfig.setComputationClass(IntNoOpComputation.class);
    GiraphConstants.AUTHENTICATE.set(tmpConfig, true);
    conf = new ImmutableClassesGiraphConfiguration(tmpConfig);
  }

  /**
   * Test connecting a single client to a single server.
   *
   * @throws IOException
   */
  @Test
  public void connectSingleClientServer() throws IOException {
    @SuppressWarnings("rawtypes")
    Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(conf);

    ServerData<IntWritable, IntWritable, IntWritable> serverData =
        MockUtils.createNewServerData(conf, context);

    SaslServerHandler.Factory mockedSaslServerFactory =
        Mockito.mock(SaslServerHandler.Factory.class);

    SaslServerHandler mockedSaslServerHandler =
        Mockito.mock(SaslServerHandler.class);
    when(mockedSaslServerFactory.newHandler(conf)).
        thenReturn(mockedSaslServerHandler);

    WorkerInfo workerInfo = new WorkerInfo();
    workerInfo.setTaskId(-1);
    NettyServer server =
        new NettyServer(conf,
            new WorkerRequestServerHandler.Factory(serverData),
            workerInfo,
            context,
            mockedSaslServerFactory,
            new MockExceptionHandler());
    server.start();
    workerInfo.setInetSocketAddress(server.getMyAddress(), server.getLocalHostOrIp());

    NettyClient client = new NettyClient(context, conf, new WorkerInfo(),
        new MockExceptionHandler());
    server.setFlowControl(client.getFlowControl());
    client.connectAllAddresses(Lists.<WorkerInfo>newArrayList(workerInfo));

    client.stop();
    server.stop();
  }
}
