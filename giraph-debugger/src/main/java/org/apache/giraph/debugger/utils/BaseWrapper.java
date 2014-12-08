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
package org.apache.giraph.debugger.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Base class for all wrapper classes that wrap a protobuf.
 *
 * author: semihsalihoglu
 */
public abstract class BaseWrapper {

  /**
   * @param <U> type of the upperBound class.
   * @param clazz a {@link Class} object that will be cast.
   * @param upperBound another {@link Class} object that clazz will be cast
   *        into.
   * @return clazz cast to upperBound.
   */
  @SuppressWarnings("unchecked")
  protected <U> Class<U> castClassToUpperBound(Class<?> clazz,
    Class<U> upperBound) {
    if (!upperBound.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("The class " + clazz.getName() +
        " is not a subclass of " + upperBound.getName());
    }
    return (Class<U>) clazz;
  }

  /**
   * Utility method to read the contents of a {@link ByteString} to the given
   * {@link Writable}.
   * @param byteString a {@link ByteString} object.
   * @param writable a {@link Writable} object.
   */
  void fromByteString(ByteString byteString, Writable writable) {
    if (writable != null) {
      WritableUtils.readFieldsFromByteArray(byteString.toByteArray(), writable);
    }
  }

  /**
   * @param writable a {@link Writable} object.
   * @return the contents of writable as {@link ByteString}.
   */
  ByteString toByteString(Writable writable) {
    return ByteString.copyFrom(WritableUtils.writeToByteArray(writable));
  }

  /**
   * Saves this wrapper object to a file.
   * @param fileName the full path of the file to save this wrapper object.
   * @throws IOException thrown when there is an exception during the writing.
   */
  public void save(String fileName) throws IOException {
    try (FileOutputStream output = new FileOutputStream(fileName)) {
      buildProtoObject().writeTo(output);
      output.close();
    }
  }


  /**
   * Saves this wrapper object to a file in HDFS.
   * @param fs {@link FileSystem} to use for saving to HDFS.
   * @param fileName the full path of the file to save this wrapper object.
   * @throws IOException thrown when there is an exception during the writing.
   */
  public void saveToHDFS(FileSystem fs, String fileName) throws IOException {
    AsyncHDFSWriteService.writeToHDFS(buildProtoObject(), fs, fileName);
  }

  /**
   * @return the protobuf representing this wrapper object.
   */
  public abstract GeneratedMessage buildProtoObject();

  /**
   * Loads a protocol buffer stored in a file into this wrapper object.
   * @param fileName the full path of the file where the protocol buffer is
   * stored.
   */
  public void load(String fileName) throws ClassNotFoundException, IOException,
    InstantiationException, IllegalAccessException {
    try (FileInputStream inputStream = new FileInputStream(fileName)) {
      loadFromProto(parseProtoFromInputStream(inputStream));
    }
  }

  /**
   * Loads a protocol buffer stored in a file in HDFS into this wrapper object.
   * @param fs {@link FileSystem} to use for reading from HDFS.
   * @param fileName the full path of the file where the protocol buffer is
   * stored.
   */
  public void loadFromHDFS(FileSystem fs, String fileName)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    try (FSDataInputStream inputStream = fs.open(new Path(fileName))) {
      loadFromProto(parseProtoFromInputStream(inputStream));
    }
  }

  /**
   * Constructs a protobuf representing this wrapper object from an
   * {@link InputStream}.
   * @param inputStream {@link InputStream} containing the contents of this
   * wrapper object.
   * @return the protobuf version of this wrapper object.
   */
  public abstract GeneratedMessage parseProtoFromInputStream(
    InputStream inputStream) throws IOException;

  /**
   * Constructs this wrapper object from a protobuf.
   * @param protoObject protobuf to read when constructing this wrapper object.
   */
  public abstract void loadFromProto(GeneratedMessage protoObject)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException;

  /**
   * Add given URLs to the CLASSPATH before loading from HDFS. To do so, we hack
   * the system class loader, assuming it is an URLClassLoader.
   *
   * XXX Setting the currentThread's context class loader has no effect on
   * Class#forName().
   *
   * @see http://stackoverflow.com/a/12963811/390044
   * @param fs {@link FileSystem} to use for reading from HDFS.
   * @param fileName the name of the file in HDFS.
   * @param classPaths a possible list of class paths that may contain the
   *        directories containing the file.
   */
  public void loadFromHDFS(FileSystem fs, String fileName, URL... classPaths)
    throws ClassNotFoundException, InstantiationException,
    IllegalAccessException, IOException {
    for (URL url : classPaths) {
      addPath(url);
    }
    loadFromHDFS(fs, fileName);
  }

  /**
   * @param u
   *          the URL to add to the CLASSPATH
   * @see http://stackoverflow.com/a/252967/390044
   */
  private static void addPath(URL u) {
    // need to do add path to Classpath with reflection since the
    // URLClassLoader.addURL(URL url) method is protected:
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    if (cl instanceof URLClassLoader) {
      URLClassLoader urlClassLoader = (URLClassLoader) cl;
      Class<URLClassLoader> urlClass = URLClassLoader.class;
      try {
        Method method = urlClass.getDeclaredMethod("addURL",
          new Class[] { URL.class });
        method.setAccessible(true);
        method.invoke(urlClassLoader, u);
      } catch (NoSuchMethodException | SecurityException |
        IllegalAccessException | IllegalArgumentException |
        InvocationTargetException e) {
        throw new IllegalStateException("Cannot add URL to system ClassLoader",
          e);
      }
    } else {
      throw new IllegalStateException(
        "Cannot add URL to system ClassLoader of type " +
          cl.getClass().getSimpleName());
    }
  }
}
