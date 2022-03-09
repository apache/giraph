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

package org.apache.giraph.io.formats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The text output format used for Giraph text writing.
 */
public abstract class GiraphTextOutputFormat
  extends TextOutputFormat<Text, Text> {

  @Override
  public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
    throws IOException, InterruptedException {
    String extension = "";
    CompressionCodec codec = null;
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);

    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(job, GzipCodec.class);
      codec =
        (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);

    /* adjust the path */
    FSDataOutputStream fileOut;
    FileSystem fs = file.getFileSystem(conf);
    String subdir = getSubdir();
    if (!subdir.isEmpty()) {
      Path subdirPath = new Path(subdir);
      Path subdirAbsPath = new Path(file.getParent(), subdirPath);
      Path vertexFile = new Path(subdirAbsPath, file.getName());
      fileOut = fs.create(vertexFile, false);
    } else {
      fileOut = fs.create(file, false);
    }

    String separator = "\t";

    if (!isCompressed) {
      return new LineRecordWriter<Text, Text>(fileOut, separator);
    } else {
      DataOutputStream out =
        new DataOutputStream(codec.createOutputStream(fileOut));
      return new LineRecordWriter<Text, Text>(out, separator);
    }
  }

  /**
   * This function is used to provide an additional path level to keep
   * different text outputs into different directories.
   *
   * @return  the subdirectory to be created under the output path
   */
  protected abstract String getSubdir();
}
