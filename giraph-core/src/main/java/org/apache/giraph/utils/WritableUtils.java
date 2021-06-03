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

import static org.apache.hadoop.util.ReflectionUtils.newInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.factories.ValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.giraph.zk.ZooKeeperExt.PathStat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Helper static methods for working with Writable objects.
 */
public class WritableUtils {
  /**
   * Don't construct.
   */
  private WritableUtils() { }

  /**
   * Instantiate a new Writable, checking for NullWritable along the way.
   *
   * @param klass Class
   * @param <W> type
   * @return new instance of class
   */
  public static <W extends Writable> W createWritable(Class<W> klass) {
    return createWritable(klass, null);
  }

  /**
   * Instantiate a new Writable, checking for NullWritable along the way.
   *
   * @param klass Class
   * @param configuration Configuration
   * @param <W> type
   * @return new instance of class
   */
  public static <W extends Writable> W createWritable(
      Class<W> klass,
      ImmutableClassesGiraphConfiguration configuration) {
    W result;
    if (NullWritable.class.equals(klass)) {
      result = (W) NullWritable.get();
    } else {
      result = ReflectionUtils.newInstance(klass);
    }
    ConfigurationUtils.configureIfPossible(result, configuration);
    return result;
  }


  /**
   * Read fields from byteArray to a Writeable object.
   *
   * @param byteArray Byte array to find the fields in.
   * @param writableObjects Objects to fill in the fields.
   */
  public static void readFieldsFromByteArray(
      byte[] byteArray, Writable... writableObjects) {
    DataInputStream inputStream =
      new DataInputStream(new ByteArrayInputStream(byteArray));
    try {
      for (Writable writableObject : writableObjects) {
        writableObject.readFields(inputStream);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "readFieldsFromByteArray: IOException", e);
    }
  }

  /**
   * Read fields from a ZooKeeper znode.
   *
   * @param zkExt ZooKeeper instance.
   * @param zkPath Path of znode.
   * @param watch Add a watch?
   * @param stat Stat of znode if desired.
   * @param writableObjects Objects to read into.
   */
  public static void readFieldsFromZnode(ZooKeeperExt zkExt,
                                         String zkPath,
                                         boolean watch,
                                         Stat stat,
                                         Writable... writableObjects) {
    try {
      byte[] zkData = zkExt.getData(zkPath, false, stat);
      readFieldsFromByteArray(zkData, writableObjects);
    } catch (KeeperException e) {
      throw new IllegalStateException(
        "readFieldsFromZnode: KeeperException on " + zkPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
        "readFieldsFromZnode: InterrruptedStateException on " + zkPath, e);
    }
  }

  /**
   * Write object to a byte array.
   *
   * @param writableObjects Objects to write from.
   * @return Byte array with serialized object.
   */
  public static byte[] writeToByteArray(Writable... writableObjects) {
    ByteArrayOutputStream outputStream =
        new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outputStream);
    try {
      for (Writable writableObject : writableObjects) {
        writableObject.write(output);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "writeToByteArray: IOStateException", e);
    }
    return outputStream.toByteArray();
  }

  /**
   * Read fields from byteArray to a Writeable object, skipping the size.
   * Serialization method is choosable
   *
   * @param byteArray Byte array to find the fields in.
   * @param writableObject Object to fill in the fields.
   * @param unsafe Use unsafe deserialization
   */
  public static void readFieldsFromByteArrayWithSize(
      byte[] byteArray, Writable writableObject, boolean unsafe) {
    ExtendedDataInput extendedDataInput;
    if (unsafe) {
      extendedDataInput = new UnsafeByteArrayInputStream(byteArray);
    } else {
      extendedDataInput = new ExtendedByteArrayDataInput(byteArray);
    }
    try {
      extendedDataInput.readInt();
      writableObject.readFields(extendedDataInput);
    } catch (IOException e) {
      throw new IllegalStateException(
          "readFieldsFromByteArrayWithSize: IOException", e);
    }
  }

  /**
   * Write object to a byte array with the first 4 bytes as the size of the
   * entire buffer (including the size).
   *
   * @param writableObject Object to write from.
   * @param unsafe Use unsafe serialization?
   * @return Byte array with serialized object.
   */
  public static byte[] writeToByteArrayWithSize(Writable writableObject,
                                                boolean unsafe) {
    return writeToByteArrayWithSize(writableObject, null, unsafe);
  }

  /**
   * Write object to a byte array with the first 4 bytes as the size of the
   * entire buffer (including the size).
   *
   * @param writableObject Object to write from.
   * @param buffer Use this buffer instead
   * @param unsafe Use unsafe serialization?
   * @return Byte array with serialized object.
   */
  public static byte[] writeToByteArrayWithSize(Writable writableObject,
                                                byte[] buffer,
                                                boolean unsafe) {
    ExtendedDataOutput extendedDataOutput;
    if (unsafe) {
      extendedDataOutput = new UnsafeByteArrayOutputStream(buffer);
    } else {
      extendedDataOutput = new ExtendedByteArrayDataOutput(buffer);
    }
    try {
      extendedDataOutput.writeInt(-1);
      writableObject.write(extendedDataOutput);
      extendedDataOutput.writeInt(0, extendedDataOutput.getPos());
    } catch (IOException e) {
      throw new IllegalStateException("writeToByteArrayWithSize: " +
          "IOException", e);
    }

    return extendedDataOutput.getByteArray();
  }

  /**
   * Write object to a ZooKeeper znode.
   *
   * @param zkExt ZooKeeper instance.
   * @param zkPath Path of znode.
   * @param version Version of the write.
   * @param writableObjects Objects to write from.
   * @return Path and stat information of the znode.
   */
  public static PathStat writeToZnode(ZooKeeperExt zkExt,
                                      String zkPath,
                                      int version,
                                      Writable... writableObjects) {
    try {
      byte[] byteArray = writeToByteArray(writableObjects);
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

  /**
   * Write list of object to a byte array.
   *
   * @param writableList List of object to write from.
   * @return Byte array with serialized objects.
   */
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

  /**
   * Write list of objects to a ZooKeeper znode.
   *
   * @param zkExt ZooKeeper instance.
   * @param zkPath Path of znode.
   * @param version Version of the write.
   * @param writableList List of objects to write from.
   * @return Path and stat information of the znode.
   */
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

  /**
   * Read fields from byteArray to a list of objects.
   *
   * @param byteArray Byte array to find the fields in.
   * @param writableClass Class of the objects to instantiate.
   * @param conf Configuration used for instantiation (i.e Configurable)
   * @param <T> Object type
   * @return List of objects.
   */
  public static <T extends Writable> List<T> readListFieldsFromByteArray(
      byte[] byteArray,
      Class<? extends T> writableClass,
      Configuration conf) {
    try {
      DataInputStream inputStream =
          new DataInputStream(new ByteArrayInputStream(byteArray));
      int size = inputStream.readInt();
      List<T> writableList = new ArrayList<T>(size);
      for (int i = 0; i < size; ++i) {
        T writable = newInstance(writableClass, conf);
        writable.readFields(inputStream);
        writableList.add(writable);
      }
      return writableList;
    } catch (IOException e) {
      throw new IllegalStateException(
          "readListFieldsFromZnode: IOException", e);
    }
  }

  /**
   * Read fields from a ZooKeeper znode into a list of objects.
   *
   * @param zkExt ZooKeeper instance.
   * @param zkPath Path of znode.
   * @param watch Add a watch?
   * @param stat Stat of znode if desired.
   * @param writableClass Class of the objects to instantiate.
   * @param conf Configuration used for instantiation (i.e Configurable)
   * @param <T> Object type
   * @return List of objects.
   */
  public static <T extends Writable> List<T> readListFieldsFromZnode(
      ZooKeeperExt zkExt,
      String zkPath,
      boolean watch,
      Stat stat,
      Class<? extends T> writableClass,
      Configuration conf) {
    try {
      byte[] zkData = zkExt.getData(zkPath, false, stat);
      return WritableUtils.<T>readListFieldsFromByteArray(zkData,
          writableClass, conf);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "readListFieldsFromZnode: KeeperException on " + zkPath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "readListFieldsFromZnode: InterruptedException on " + zkPath,
          e);
    }
  }

  /**
   * Write ExtendedDataOutput to DataOutput
   *
   * @param extendedDataOutput ExtendedDataOutput to write
   * @param out DataOutput to write to
   */
  public static void writeExtendedDataOutput(
      ExtendedDataOutput extendedDataOutput, DataOutput out)
    throws IOException {
    out.writeInt(extendedDataOutput.getPos());
    out.write(
        extendedDataOutput.getByteArray(), 0, extendedDataOutput.getPos());
  }

  /**
   * Read ExtendedDataOutput from DataInput
   *
   * @param in DataInput to read from
   * @param conf Configuration
   * @return ExtendedDataOutput read
   */
  public static ExtendedDataOutput readExtendedDataOutput(DataInput in,
      ImmutableClassesGiraphConfiguration conf) throws IOException {
    int size = in.readInt();
    byte[] buf = new byte[size];
    in.readFully(buf);
    return conf.createExtendedDataOutput(buf, size);
  }

  /**
   * Write vertex data to byte array with the first 4 bytes as the size of the
   * entire buffer (including the size).
   *
   * @param vertex Vertex to write from.
   * @param buffer Use this buffer instead
   * @param unsafe Use unsafe serialization?
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   * @return Byte array with serialized object.
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable> byte[] writeVertexToByteArray(
      Vertex<I, V, E> vertex,
      byte[] buffer,
      boolean unsafe,
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    ExtendedDataOutput extendedDataOutput;
    if (unsafe) {
      extendedDataOutput = new UnsafeByteArrayOutputStream(buffer);
    } else {
      extendedDataOutput = new ExtendedByteArrayDataOutput(buffer);
    }
    try {
      extendedDataOutput.writeInt(-1);
      writeVertexToDataOutput(extendedDataOutput, vertex, conf);
      extendedDataOutput.writeInt(0, extendedDataOutput.getPos());
    } catch (IOException e) {
      throw new IllegalStateException("writeVertexToByteArray: " +
          "IOException", e);
    }

    return extendedDataOutput.getByteArray();
  }

  /**
   * Write vertex data to byte array with the first 4 bytes as the size of the
   * entire buffer (including the size).
   *
   * @param vertex Vertex to write from.
   * @param unsafe Use unsafe serialization?
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   * @return Byte array with serialized object.
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable> byte[] writeVertexToByteArray(
      Vertex<I, V, E> vertex,
      boolean unsafe,
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    return writeVertexToByteArray(vertex, null, unsafe, conf);
  }

  /**
  * Read vertex data from byteArray to a Writeable object, skipping the size.
  * Serialization method is choosable. Assumes the vertex has already been
  * initialized and contains values for Id, value, and edges.
  *
  * @param byteArray Byte array to find the fields in.
  * @param vertex Vertex to fill in the fields.
  * @param unsafe Use unsafe deserialization
  * @param <I> Vertex id
  * @param <V> Vertex value
  * @param <E> Edge value
  * @param conf Configuration
  */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable> void reinitializeVertexFromByteArray(
      byte[] byteArray,
      Vertex<I, V, E> vertex,
      boolean unsafe,
      ImmutableClassesGiraphConfiguration<I, V, E> conf) {
    ExtendedDataInput extendedDataInput;
    if (unsafe) {
      extendedDataInput = new UnsafeByteArrayInputStream(byteArray);
    } else {
      extendedDataInput = new ExtendedByteArrayDataInput(byteArray);
    }
    try {
      extendedDataInput.readInt();
      reinitializeVertexFromDataInput(extendedDataInput, vertex, conf);
    } catch (IOException e) {
      throw new IllegalStateException(
          "readFieldsFromByteArrayWithSize: IOException", e);
    }
  }

  /**
   * Write an edge to an output stream.
   *
   * @param out Data output
   * @param edge Edge to write
   * @param <I> Vertex id
   * @param <E> Edge value
   * @throws IOException
   */
  public static <I extends WritableComparable, E extends Writable>
  void writeEdge(DataOutput out, Edge<I, E> edge) throws IOException {
    edge.getTargetVertexId().write(out);
    edge.getValue().write(out);
  }

  /**
   * Read an edge from an input stream.
   *
   * @param in Data input
   * @param edge Edge to fill in-place
   * @param <I> Vertex id
   * @param <E> Edge value
   * @throws IOException
   */
  public static <I extends WritableComparable, E extends Writable>
  void readEdge(DataInput in, Edge<I, E> edge) throws IOException {
    edge.getTargetVertexId().readFields(in);
    edge.getValue().readFields(in);
  }

  /**
   * Reads data from input stream to initialize Vertex. Assumes the vertex has
   * already been initialized and contains values for Id, value, and edges.
   *
   * @param input The input stream
   * @param vertex The vertex to initialize
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable> void reinitializeVertexFromDataInput(
      DataInput input,
      Vertex<I, V, E> vertex,
      ImmutableClassesGiraphConfiguration<I, V, E> conf)
    throws IOException {
    vertex.getId().readFields(input);
    vertex.getValue().readFields(input);
    ((OutEdges<I, E>) vertex.getEdges()).readFields(input);
    if (input.readBoolean()) {
      vertex.voteToHalt();
    } else {
      vertex.wakeUp();
    }
  }

  /**
   * Reads data from input stream to initialize Vertex.
   *
   * @param input The input stream
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   * @return The vertex
   * @throws IOException
   */
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable> Vertex<I, V, E>
  readVertexFromDataInput(
      DataInput input,
      ImmutableClassesGiraphConfiguration<I, V, E> conf)
    throws IOException {
    Vertex<I, V, E> vertex = conf.createVertex();
    I id = conf.createVertexId();
    V value = conf.createVertexValue();
    OutEdges<I, E> edges = conf.createOutEdges();
    vertex.initialize(id, value, edges);
    reinitializeVertexFromDataInput(input, vertex, conf);
    return vertex;
  }

  /**
   * Writes Vertex data to output stream.
   *
   * @param output the output stream
   * @param vertex The vertex to serialize
   * @param conf Configuration
   * @param <I> Vertex id
   * @param <V> Vertex value
   * @param <E> Edge value
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <I extends WritableComparable, V extends Writable,
  E extends Writable> void writeVertexToDataOutput(
      DataOutput output,
      Vertex<I, V, E> vertex,
      ImmutableClassesGiraphConfiguration<I, V, E> conf)
    throws IOException {
    vertex.getId().write(output);
    vertex.getValue().write(output);
    ((OutEdges<I, E>) vertex.getEdges()).write(output);
    output.writeBoolean(vertex.isHalted());
  }

  /**
   * Write class to data output. Also handles the case when class is null.
   *
   * @param clazz Class
   * @param output Data output
   * @param <T> Class type
   */
  public static <T> void writeClass(Class<T> clazz,
      DataOutput output) throws IOException {
    output.writeBoolean(clazz != null);
    if (clazz != null) {
      output.writeUTF(clazz.getName());
    }
  }

  /**
   * Read class from data input.
   * Matches {@link #writeClass(Class, DataOutput)}.
   *
   * @param input Data input
   * @param <T> Class type
   * @return Class, or null if null was written
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> readClass(DataInput input) throws IOException {
    if (input.readBoolean()) {
      String className = input.readUTF();
      try {
        return (Class<T>) Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("readClass: No class found " +
            className);
      }
    } else {
      return null;
    }
  }

  /**
   * Write object to output stream
   * @param object Object
   * @param output Output stream
   * @throws IOException
   */
  public static void writeWritableObject(
    Writable object, DataOutput output)
    throws IOException {
    output.writeBoolean(object != null);
    if (object != null) {
      output.writeUTF(object.getClass().getName());
      object.write(output);
    }
  }

  /**
   * Reads object from input stream
   * @param input Input stream
   * @param conf Configuration
   * @param <T> Object type
   * @return Object
   * @throws IOException
   */
  public static <T extends Writable>
  T readWritableObject(DataInput input,
      ImmutableClassesGiraphConfiguration conf) throws IOException {
    if (input.readBoolean()) {
      String className = input.readUTF();
      try {
        T object =
            (T) ReflectionUtils.newInstance(Class.forName(className), conf);
        object.readFields(input);
        return object;
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("readWritableObject: No class found " +
            className);
      }
    } else {
      return null;
    }
  }

  /**
   * Writes a list of Writable objects into output stream.
   * This method is trying to optimize space occupied by class information only
   * storing class object if it is different from the previous one
   * as in most cases arrays tend to have objects of the same type inside.
   * @param list serialized object
   * @param output the output stream
   * @throws IOException
   */
  public static void writeList(List<? extends Writable> list, DataOutput output)
    throws IOException {
    output.writeBoolean(list != null);
    if (list != null) {
      output.writeInt(list.size());
      Class<? extends Writable> clazz = null;
      for (Writable element : list) {
        output.writeBoolean(element == null);
        if (element != null) {
          if (element.getClass() != clazz) {
            clazz = element.getClass();
            output.writeBoolean(true);
            writeClass(clazz, output);
          } else {
            output.writeBoolean(false);
          }
          element.write(output);
        }
      }
    }
  }

  /**
   * Reads list of Writable objects from data input stream.
   * Input stream should have class information along with object data.
   * @param input input stream
   * @return deserialized list
   * @throws IOException
   */
  public static List<? extends Writable> readList(DataInput input)
    throws IOException {
    try {
      List<Writable> res = null;
      if (input.readBoolean()) {
        int size = input.readInt();
        res = new ArrayList<>(size);
        Class<? extends Writable> clazz = null;
        for (int i = 0; i < size; i++) {
          boolean isNull = input.readBoolean();
          if (isNull) {
            res.add(null);
          } else {
            boolean hasClassInfo = input.readBoolean();
            if (hasClassInfo) {
              clazz = readClass(input);
            }
            Writable element = clazz.newInstance();
            element.readFields(input);
            res.add(element);
          }
        }
      }
      return res;

    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException("unable to instantiate object", e);
    }
  }

  /**
   * Writes primitive int array of ints into output stream.
   * Array can be null or empty.
   * @param array array to be written
   * @param dataOutput output stream
   * @throws IOException
   */
  public static void writeIntArray(int[] array, DataOutput dataOutput)
    throws IOException {
    if (array != null) {
      dataOutput.writeInt(array.length);
      for (int r : array) {
        dataOutput.writeInt(r);
      }
    } else {
      dataOutput.writeInt(-1);
    }
  }

  /**
   * Reads primitive int array from input stream.
   * @param dataInput input stream to read from
   * @return may return null or empty array.
   * @throws IOException
   */
  public static int[] readIntArray(DataInput dataInput)
    throws IOException {
    int [] res = null;
    int size = dataInput.readInt();
    if (size >= 0) {
      res = new int[size];
      for (int i = 0; i < size; i++) {
        res[i] = dataInput.readInt();
      }
    }
    return res;
  }

  /**
   * Writes primitive long array of ints into output stream.
   * Array can be null or empty.
   * @param array array to be written
   * @param dataOutput output stream
   * @throws IOException
   */
  public static void writeLongArray(DataOutput dataOutput, long[] array)
    throws IOException {
    if (array != null) {
      dataOutput.writeInt(array.length);
      for (long r : array) {
        dataOutput.writeLong(r);
      }
    } else {
      dataOutput.writeInt(-1);
    }
  }
  /**
   * Reads primitive long array from input stream.
   * @param dataInput input stream to read from
   * @return may return null or empty array.
   * @throws IOException
   */
  public static long[] readLongArray(DataInput dataInput)
    throws IOException {
    long [] res = null;
    int size = dataInput.readInt();
    if (size >= 0) {
      res = new long[size];
      for (int i = 0; i < size; i++) {
        res[i] = dataInput.readLong();
      }
    }
    return res;
  }

  /**
   * Writes enum into a stream, by serializing class name and it's index
   * @param enumValue Enum value
   * @param output Output stream
   * @param <T> Enum type
   */
  public static <T extends Enum<T>> void writeEnum(T enumValue,
      DataOutput output) throws IOException {
    writeClass(
        enumValue != null ? enumValue.getDeclaringClass() : null, output);
    if (enumValue != null) {
      Varint.writeUnsignedVarInt(enumValue.ordinal(), output);
    }
  }

  /**
   * Reads enum from the stream, serialized by writeEnum
   * @param input Input stream
   * @param <T> Enum type
   * @return Enum value
   */
  public static <T extends Enum<T>> T readEnum(DataInput input) throws
      IOException {
    Class<T> clazz = readClass(input);
    if (clazz != null) {
      int ordinal = Varint.readUnsignedVarInt(input);
      try {
        T[] values = (T[]) clazz.getDeclaredMethod("values").invoke(null);
        return values[ordinal];
      } catch (IllegalAccessException | IllegalArgumentException |
          InvocationTargetException | NoSuchMethodException |
          SecurityException e) {
        throw new IOException("Cannot read enum", e);
      }
    } else {
      return null;
    }
  }


  /**
   * Copy {@code from} into {@code to}, by serializing and deserializing it.
   * Since it is creating streams inside, it's mostly useful for
   * tests/non-performant code.
   *
   * @param from Object to copy from
   * @param to Object to copy into
   * @param <T> Type of the object
   */
  public static <T extends Writable> void copyInto(T from, T to) {
    copyInto(from, to, false);
  }

  /**
   * Copy {@code from} into {@code to}, by serializing and deserializing it.
   * Since it is creating streams inside, it's mostly useful for
   * tests/non-performant code.
   *
   * @param from Object to copy from
   * @param to Object to copy into
   * @param checkOverRead if true, will add one more byte at the end of writing,
   *                      to make sure read is not touching it. Useful for tests
   * @param <T> Type of the object
   */
  public static <T extends Writable> void copyInto(
      T from, T to, boolean checkOverRead) {
    try {
      if (from.getClass() != to.getClass()) {
        throw new RuntimeException(
            "Trying to copy from " + from.getClass() +
            " into " + to.getClass());
      }

      UnsafeByteArrayOutputStream out = new UnsafeByteArrayOutputStream();
      from.write(out);
      if (checkOverRead) {
        out.writeByte(0);
      }

      UnsafeByteArrayInputStream in =
          new UnsafeByteArrayInputStream(out.getByteArray(), 0, out.getPos());
      to.readFields(in);

      if (in.available() != (checkOverRead ? 1 : 0)) {
        throw new RuntimeException(
            "Serialization encountered issues with " + from.getClass() + ", " +
            (in.available() - (checkOverRead ? 1 : 0)) + " fewer bytes read");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a copy of Writable object, by serializing and deserializing it.
   *
   * @param reusableOut Reusable output stream to serialize into
   * @param reusableIn Reusable input stream to deserialize out of
   * @param original Original value of which to make a copy
   * @param conf Configuration
   * @param <T> Type of the object
   * @return Copy of the original value
   */
  public static <T extends Writable> T createCopy(
      UnsafeByteArrayOutputStream reusableOut,
      UnsafeReusableByteArrayInput reusableIn, T original,
      ImmutableClassesGiraphConfiguration conf) {
    T copy = (T) createWritable(original.getClass(), conf);

    try {
      reusableOut.reset();
      original.write(reusableOut);
      reusableIn.initialize(
          reusableOut.getByteArray(), 0, reusableOut.getPos());
      copy.readFields(reusableIn);

      if (reusableIn.available() != 0) {
        throw new RuntimeException("Serialization of " +
            original.getClass() + " encountered issues, " +
            reusableIn.available() + " bytes left to be read");
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "IOException occurred while trying to create a copy " +
          original.getClass(), e);
    }
    return copy;
  }

  /**
   * Create a copy of Writable object, by serializing and deserializing it.
   *
   * @param original Original value of which to make a copy
   * @return Copy of the original value
   * @param <T> Type of the object
   */
  public static final <T extends Writable> T createCopy(T original) {
    return (T) createCopy(original, original.getClass(), null);
  }

  /**
   * Create a copy of Writable object, by serializing and deserializing it.
   *
   * @param original Original value of which to make a copy
   * @param outputClass Expected copy class, needs to match original
   * @param conf Configuration
   * @return Copy of the original value
   * @param <T> Type of the object
   */
  public static final <T extends Writable>
  T createCopy(T original, Class<? extends T> outputClass,
      ImmutableClassesGiraphConfiguration conf) {
    T result = WritableUtils.createWritable(outputClass, conf);
    copyInto(original, result);
    return result;
  }

  /**
   * Create a copy of Writable object, by serializing and deserializing it.
   *
   * @param original Original value of which to make a copy
   * @param classFactory Factory to create new empty object from
   * @param conf Configuration
   * @return Copy of the original value
   * @param <T> Type of the object
   */
  public static final <T extends Writable>
  T createCopy(T original, ValueFactory<T> classFactory,
      ImmutableClassesGiraphConfiguration conf) {
    T result = classFactory.newInstance();
    copyInto(original, result);
    return result;
  }

  /**
   * Serialize given writable object, and return it's size.
   *
   * @param w Writable object
   * @return it's size after serialization
   */
  public static int size(Writable w) {
    try {
      ExtendedByteArrayDataOutput out = new ExtendedByteArrayDataOutput();
      w.write(out);
      return out.getPos();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize given writable to byte array,
   * using new instance of ExtendedByteArrayDataOutput.
   *
   * @param w Writable object
   * @return array of bytes
   * @param <T> Type of the object
   */
  public static <T extends Writable> byte[] toByteArray(T w) {
    try {
      ExtendedByteArrayDataOutput out = new ExtendedByteArrayDataOutput();
      w.write(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize from given byte array into given writable,
   * using new instance of ExtendedByteArrayDataInput.
   *
   * @param data Byte array representing writable
   * @param to Object to fill
   * @param <T> Type of the object
   */
  public static <T extends Writable> void fromByteArray(byte[] data, T to) {
    try {
      ExtendedByteArrayDataInput in =
          new ExtendedByteArrayDataInput(data, 0, data.length);
      to.readFields(in);

      if (in.available() != 0) {
        throw new RuntimeException(
            "Serialization encountered issues, " + in.available() +
            " bytes left to be read");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize given writable to byte array,
   * using new instance of UnsafeByteArrayOutputStream.
   *
   * @param w Writable object
   * @return array of bytes
   * @param <T> Type of the object
   */
  public static <T extends Writable> byte[] toByteArrayUnsafe(T w) {
    try {
      UnsafeByteArrayOutputStream out = new UnsafeByteArrayOutputStream();
      w.write(out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deserialize from given byte array into given writable,
   * using given reusable UnsafeReusableByteArrayInput.
   *
   * @param data Byte array representing writable
   * @param to Object to fill
   * @param reusableInput Reusable input to use
   * @param <T> Type of the object
   */
  public static <T extends Writable> void fromByteArrayUnsafe(
      byte[] data, T to, UnsafeReusableByteArrayInput reusableInput) {
    try {
      reusableInput.initialize(data, 0, data.length);
      to.readFields(reusableInput);

      if (reusableInput.available() != 0) {
        throw new RuntimeException(
            "Serialization encountered issues, " + reusableInput.available() +
            " bytes left to be read");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * First write a boolean saying whether an object is not null,
   * and if it's not write the object
   *
   * @param object Object to write
   * @param out DataOutput to write to
   * @param <T> Object type
   */
  public static <T extends Writable> void writeIfNotNullAndObject(T object,
      DataOutput out) throws IOException {
    out.writeBoolean(object != null);
    if (object != null) {
      object.write(out);
    }
  }

  /**
   * First read a boolean saying whether an object is not null,
   * and if it's not read the object
   *
   * @param reusableObject Reuse this object instance
   * @param objectClass Class of the object, to create if reusableObject is null
   * @param in DataInput to read from
   * @param <T> Object type
   * @return Object, or null
   */
  public static <T extends Writable> T readIfNotNullAndObject(T reusableObject,
      Class<T> objectClass, DataInput in) throws IOException {
    if (in.readBoolean()) {
      if (reusableObject == null) {
        reusableObject = ReflectionUtils.newInstance(objectClass);
      }
      reusableObject.readFields(in);
      return reusableObject;
    } else {
      return null;
    }
  }

}
