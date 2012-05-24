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

package org.apache.giraph.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperExt.PathStat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Helper static methods for working with Writable objects.
 */
public class WritableUtils {
    public static void readFieldsFromByteArray(
            byte[] byteArray, Writable writableObject) {
        DataInputStream inputStream =
            new DataInputStream(new ByteArrayInputStream(byteArray));
        try {
            writableObject.readFields(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException(
                "readFieldsFromByteArray: IOException", e);
        }
    }

    public static void readFieldsFromZnode(ZooKeeperExt zkExt,
                                           String zkPath,
                                           boolean watch,
                                           Stat stat,
                                           Writable writableObject) {
        try {
            byte[] zkData = zkExt.getData(zkPath, false, stat);
            readFieldsFromByteArray(zkData, writableObject);
        } catch (KeeperException e) {
            throw new IllegalStateException(
               "readFieldsFromZnode: KeeperException on " + zkPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
               "readFieldsFromZnode: InterrruptedStateException on " + zkPath,
               e);
        }
    }

    public static byte[] writeToByteArray(Writable writableObject) {
        ByteArrayOutputStream outputStream =
            new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        try {
            writableObject.write(output);
        } catch (IOException e) {
            throw new IllegalStateException(
                "writeToByteArray: IOStateException", e);
        }
        return outputStream.toByteArray();
    }

    public static PathStat writeToZnode(ZooKeeperExt zkExt,
                                        String zkPath,
                                        int version,
                                        Writable writableObject) {
        try {
            byte[] byteArray = writeToByteArray(writableObject);
            return zkExt.createOrSetExt(zkPath,
                                        byteArray,
                                        Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT,
                                        true,
                                        version);
        } catch (KeeperException e) {
            throw new IllegalStateException(
               "writeToZnode: KeeperException on " + zkPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "writeToZnode: InterruptedException on " + zkPath, e);
        }
    }

    public static byte[] writeListToByteArray(
            List<? extends Writable> writableList) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(outputStream);
        try {
            output.writeInt(writableList.size());
            for (Writable writable : writableList) {
                writable.write(output);
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "writeListToByteArray: IOException", e);
        }
        return outputStream.toByteArray();
    }

    public static PathStat writeListToZnode(
            ZooKeeperExt zkExt,
            String zkPath,
            int version,
            List<? extends Writable> writableList) {
        try {
            return zkExt.createOrSetExt(
                zkPath,
                writeListToByteArray(writableList),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                true,
                version);
        } catch (KeeperException e) {
            throw new IllegalStateException(
               "writeListToZnode: KeeperException on " + zkPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "writeListToZnode: InterruptedException on " + zkPath, e);
        }
    }

    public static List<? extends Writable> readListFieldsFromByteArray(
            byte[] byteArray,
            Class<? extends Writable> writableClass,
            Configuration conf) {
        try {
            DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(byteArray));
            int size = inputStream.readInt();
            List<Writable> writableList = new ArrayList<Writable>(size);
            for (int i = 0; i < size; ++i) {
                Writable writable =
                    ReflectionUtils.newInstance(writableClass, conf);
                writable.readFields(inputStream);
                writableList.add(writable);
            }
            return writableList;
        } catch (IOException e) {
            throw new IllegalStateException(
                    "readListFieldsFromZnode: IOException", e);
        }
    }

    public static List<? extends Writable> readListFieldsFromZnode(
            ZooKeeperExt zkExt,
            String zkPath,
            boolean watch,
            Stat stat,
            Class<? extends Writable> writableClass,
            Configuration conf) {
        try {
            byte[] zkData = zkExt.getData(zkPath, false, stat);
            return readListFieldsFromByteArray(zkData, writableClass, conf);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                "readListFieldsFromZnode: KeeperException on " + zkPath, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "readListFieldsFromZnode: InterruptedException on " + zkPath,
                e);
        }
    }
}
