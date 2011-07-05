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

package org.apache.giraph.zk;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * ZooKeeper provides only atomic operations.  ZooKeeperExt provides additional
 * non-atomic operations that are useful.
 */
public class ZooKeeperExt extends ZooKeeper {
    /** Internal logger */
    private static final Logger LOG = Logger.getLogger(ZooKeeperExt.class);
    /** Length of the ZK sequence number */
    private static final int SEQUENCE_NUMBER_LENGTH = 10;

    public ZooKeeperExt(
        String connectString,
        int sessionTimeout,
        Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
    }

    /**
     *
     * Provides a possibility of a creating a path consisting of more than one
     * znode (not atomic).  If recursive is false, operates exactly the
     * same as create().
     *
     * @param path path to create
     * @param data data to set on the final znode
     * @param acl acls on each znode created
     * @param createMode only affects the final znode
     * @param recursive if true, creates all ancestors
     * @return Actual created path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String createExt(
        final String path,
        byte data[],
        List<ACL> acl,
        CreateMode createMode,
        boolean recursive) throws KeeperException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("createExt: Creating path " + path);
        }

        if (recursive == false) {
            return create(path, data, acl, createMode);
        }

        try {
            return create(path, data, acl, createMode);
        } catch (KeeperException.NoNodeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("createExt: Cannot directly create node " + path);
            }
        }

        int pos = path.indexOf("/", 1);
        for (; pos != -1; pos = path.indexOf("/", pos + 1)) {
            try {
                create(
                    path.substring(0, pos), null, acl, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("createExt: Znode " + path.substring(0, pos) +
                              " already exists");
                }
            }
        }
        return create(path, data, acl, createMode);
    }

    /**
     * Delete a path recursively.  When the deletion is recursive, it is a
     * non-atomic operation, hence, not part of ZooKeeper.
     * @param path path to remove (i.e. /tmp will remove /tmp/1 and /tmp/2)
     * @param version expected version (-1 for all)
     * @param recursive if true, remove all children, otherwise behave like
     *        remove()
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteExt(final String path, int version, boolean recursive)
        throws InterruptedException, KeeperException {
        if (recursive == false) {
            delete(path, version);
            return;
        }

        try {
            delete(path, version);
            return;
        } catch (KeeperException.NotEmptyException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("deleteExt: Cannot directly remove node " + path);
            }
        }

        List<String> childList = getChildren(path, false);
        for (String child : childList) {
            deleteExt(path + "/" + child, -1, true);
        }

        delete(path, version);
    }

    /**
     * Get the children of the path with extensions.
     * Extension 1: Sort the children based on sequence number
     * Extension 2: Get the full path instead of relative path
     *
     * @param path path to znode
     * @param watch set the watch?
     * @param sequenceSorted sort by the sequence number
     * @param fullPath if true, get the fully znode path back
     * @return list of children
     * @throws InterruptedException
     * @throws KeeperException
     */
    public List<String> getChildrenExt(final String path,
                                       boolean watch,
                                       boolean sequenceSorted,
                                       boolean fullPath)
        throws KeeperException, InterruptedException {
        List<String> childList = getChildren(path, watch);
        /* Sort children according to the sequence number, if desired */
        if (sequenceSorted) {
            Collections.sort(childList,
                new Comparator<String>() {
                    public int compare(String s1, String s2) {
                        if ((s1.length() <= SEQUENCE_NUMBER_LENGTH) ||
                            (s2.length() <= SEQUENCE_NUMBER_LENGTH)) {
                            throw new RuntimeException(
                                "getChildrenExt: Invalid length for sequence " +
                                " sorting > " +
                                SEQUENCE_NUMBER_LENGTH +
                                " for s1 (" +
                                s1.length() + ") or s2 (" + s2.length() + ")");
                        }
                        int s1sequenceNumber = Integer.parseInt(
                                s1.substring(s1.length() -
                                             SEQUENCE_NUMBER_LENGTH));
                        int s2sequenceNumber = Integer.parseInt(
                                s2.substring(s2.length() -
                                             SEQUENCE_NUMBER_LENGTH));
                        return s1sequenceNumber - s2sequenceNumber;
                    }
                }
            );
        }
        if (fullPath) {
            List<String> fullChildList = new ArrayList<String>();
            for (String child : childList) {
                fullChildList.add(path + "/" + child);
            }
            return fullChildList;
        }
        return childList;
    }
}
