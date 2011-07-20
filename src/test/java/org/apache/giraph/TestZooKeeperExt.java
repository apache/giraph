/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
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

package org.apache.giraph;

import java.util.List;

import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import junit.framework.TestCase;

public class TestZooKeeperExt
        extends TestCase implements Watcher {
    /** ZooKeeperExt instance */
    private ZooKeeperExt zooKeeperExt = null;
    /** ZooKeeper server list */
    private String zkList = System.getProperty("prop.zookeeper.list");

    public final String BASE_PATH = "/_zooKeeperExtTest";
    public final String FIRST_PATH = "/_first";

    public void process(WatchedEvent event) {
        return;
    }

    @Override
    public void setUp() {
        try {
            if (zkList == null) {
                return;
            }
            zooKeeperExt =
                new ZooKeeperExt(zkList, 30*1000, this);
            zooKeeperExt.deleteExt(BASE_PATH, -1, true);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("Clean start: No node " + BASE_PATH);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tearDown() {
        if (zooKeeperExt == null) {
            return;
        }
        try {
            zooKeeperExt.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void testCreateExt() throws KeeperException, InterruptedException {
        if (zooKeeperExt == null) {
            System.out.println(
                "testCreateExt: No prop.zookeeper.list set, skipping test");
            return;
        }
        System.out.println("Created: " +
            zooKeeperExt.createExt(
                BASE_PATH + FIRST_PATH,
                null,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                true));
        zooKeeperExt.delete(BASE_PATH + FIRST_PATH, -1);
        zooKeeperExt.delete(BASE_PATH, -1);
    }

    public void testDeleteExt() throws KeeperException, InterruptedException {
        if (zooKeeperExt == null) {
            System.out.println(
                "testDeleteExt: No prop.zookeeper.list set, skipping test");
            return;
        }
        zooKeeperExt.create(BASE_PATH,
                              null,
                              Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
        zooKeeperExt.create(BASE_PATH + FIRST_PATH,
                                null,
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
        try {
            zooKeeperExt.deleteExt(BASE_PATH, -1, false);
        } catch (KeeperException.NotEmptyException e) {
            System.out.println(
                "Correctly failed to delete since not recursive");
        }
        zooKeeperExt.deleteExt(BASE_PATH, -1, true);
    }

    public void testGetChildrenExt()
        throws KeeperException, InterruptedException {
        if (zooKeeperExt == null) {
           System.out.println(
               "testGetChildrenExt: No prop.zookeeper.list set, skipping test");
           return;
        }
        zooKeeperExt.create(BASE_PATH,
                              null,
                              Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
        zooKeeperExt.create(BASE_PATH + "/b",
                null,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        zooKeeperExt.create(BASE_PATH + "/a",
                null,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        zooKeeperExt.create(BASE_PATH + "/d",
                null,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        zooKeeperExt.create(BASE_PATH + "/c",
                null,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        List<String> fullPathList =
            zooKeeperExt.getChildrenExt(BASE_PATH, false, false, true);
        for (String fullPath : fullPathList) {
            assertTrue(fullPath.contains(BASE_PATH + "/"));
        }
        List<String> sequenceOrderedList =
            zooKeeperExt.getChildrenExt(BASE_PATH, false, true, true);
        for (String fullPath : sequenceOrderedList) {
            assertTrue(fullPath.contains(BASE_PATH + "/"));
        }
        assertTrue(sequenceOrderedList.size() == 4);
        assertTrue(sequenceOrderedList.get(0).contains("/b"));
        assertTrue(sequenceOrderedList.get(1).contains("/a"));
        assertTrue(sequenceOrderedList.get(2).contains("/d"));
        assertTrue(sequenceOrderedList.get(3).contains("/c"));
    }
}
