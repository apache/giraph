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

package org.apache.giraph.zk;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import static java.lang.System.out;

/** A utility class to be used to create a ZooKeeper node */
public class ZooKeeperNodeCreator implements Tool, Watcher {
  /** The configuration */
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("zk", "zkServer", true,
        "List of host:port ZooKeeper servers");
    options.addOption("n", "zkNode", true,
        "ZooKeeper node to create");

    HelpFormatter formatter = new HelpFormatter();
    if (args.length == 0) {
      formatter.printHelp(getClass().getName(), options, true);
      return 0;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    ZooKeeperExt zkExt = new ZooKeeperExt(cmd.getOptionValue("zkServer"),
        30 * 1000, 5, 1000, this);
    zkExt.createExt(cmd.getOptionValue("zkNode"), new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
    return 0;
  }

  @Override
  public void process(WatchedEvent event) {
    out.println("process: ZK event received: " + event);
  }

  /**
   * Entry point from shell script
   * @param args the command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ZooKeeperNodeCreator(), args));
  }
}
