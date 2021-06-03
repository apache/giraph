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

import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility methods for dealing with Hadoop configuration
 */
public class ConfigurationObjectUtils {
  /** Hide constructor */
  private ConfigurationObjectUtils() {
  }

  /**
   * Encode bytes to a hex String
   *
   * @param bytes byte[]
   * @return encoded String
   */
  public static String encodeBytes(byte[] bytes) {
    StringBuilder strBuf = new StringBuilder();
    for (int i = 0; i < bytes.length; i++) {
      strBuf.append((char) (((bytes[i] >> 4) & 0xF) + ('a')));
      strBuf.append((char) (((bytes[i]) & 0xF) + ('a')));
    }
    return strBuf.toString();
  }

  /**
   * Decode bytes from a hex String
   *
   * @param str String to decode
   * @return decoded byte[]
   */
  public static byte[] decodeBytes(String str) {
    byte[] bytes = new byte[str.length() / 2];
    for (int i = 0; i < str.length(); i += 2) {
      char c = str.charAt(i);
      bytes[i / 2] = (byte) ((c - 'a') << 4);
      c = str.charAt(i + 1);
      bytes[i / 2] += c - 'a';
    }
    return bytes;
  }

  /**
   * Set byte array to a conf option
   *
   * @param data Byte array
   * @param confOption Conf option
   * @param conf Configuration
   */
  public static void setByteArray(byte[] data, String confOption,
      Configuration conf) {
    conf.set(confOption, encodeBytes(data));
  }

  /**
   * Get byte array from a conf option
   *
   * @param confOption Conf option
   * @param conf Configuration
   * @return Byte array
   */
  public static byte[] getByteArray(String confOption,
      Configuration conf) {
    return decodeBytes(conf.get(confOption));
  }

  /**
   * Set object in a conf option using kryo
   *
   * @param object Object to set
   * @param confOption Conf option
   * @param conf Configuration
   * @param <T> Type of the object
   */
  public static <T> void setObjectKryo(T object, String confOption,
      Configuration conf) {
    setByteArray(WritableUtils.toByteArrayUnsafe(
            new KryoWritableWrapper<>(object)),
        confOption, conf);
  }

  /**
   * Get object from a conf option using kryo
   *
   * @param confOption Conf option
   * @param conf Configuration
   * @return Object from conf
   * @param <T> Type of the object
   */
  public static <T> T getObjectKryo(String confOption,
      Configuration conf) {
    KryoWritableWrapper<T> wrapper = new KryoWritableWrapper<>();
    WritableUtils.fromByteArrayUnsafe(
        getByteArray(confOption, conf), wrapper,
        new UnsafeReusableByteArrayInput());
    return wrapper.get();
  }
}
