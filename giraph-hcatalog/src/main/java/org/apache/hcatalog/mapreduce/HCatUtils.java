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

package org.apache.hcatalog.mapreduce;

import org.apache.giraph.io.hcatalog.GiraphHCatInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hcatalog.common.ErrorType;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility methods copied from HCatalog because of visibility restrictions.
 */
public class HCatUtils {
  /**
   * Don't instantiate.
   */
  private HCatUtils() { }

  /**
   * Returns the given InputJobInfo after populating with data queried from the
   * metadata service.
   *
   * @param conf Configuration
   * @param inputJobInfo Input job info
   * @return Populated input job info
   * @throws IOException
   */
  public static InputJobInfo getInputJobInfo(
      Configuration conf, InputJobInfo inputJobInfo)
    throws IOException {
    HiveMetaStoreClient client = null;
    HiveConf hiveConf;
    try {
      if (conf != null) {
        hiveConf = HCatUtil.getHiveConf(conf);
      } else {
        hiveConf = new HiveConf(GiraphHCatInputFormat.class);
      }
      client = HCatUtil.getHiveClient(hiveConf);
      Table table = HCatUtil.getTable(client, inputJobInfo.getDatabaseName(),
          inputJobInfo.getTableName());

      List<PartInfo> partInfoList = new ArrayList<PartInfo>();

      inputJobInfo.setTableInfo(HCatTableInfo.valueOf(table.getTTable()));
      if (table.getPartitionKeys().size() != 0) {
        // Partitioned table
        List<Partition> parts = client.listPartitionsByFilter(
            inputJobInfo.getDatabaseName(),
            inputJobInfo.getTableName(),
            inputJobInfo.getFilter(),
            (short) -1);

        if (parts != null) {
          // Default to 100,000 partitions if hive.metastore.maxpartition is not
          // defined
          int maxPart = hiveConf.getInt("hcat.metastore.maxpartitions", 100000);
          if (parts.size() > maxPart) {
            throw new HCatException(ErrorType.ERROR_EXCEED_MAXPART,
                "total number of partitions is " + parts.size());
          }

          // Populate partition info
          for (Partition ptn : parts) {
            HCatSchema schema = HCatUtil.extractSchema(
                new org.apache.hadoop.hive.ql.metadata.Partition(table, ptn));
            PartInfo partInfo = extractPartInfo(schema, ptn.getSd(),
                ptn.getParameters(), conf, inputJobInfo);
            partInfo.setPartitionValues(InternalUtil.createPtnKeyValueMap(table,
                ptn));
            partInfoList.add(partInfo);
          }
        }
      } else {
        // Non partitioned table
        HCatSchema schema = HCatUtil.extractSchema(table);
        PartInfo partInfo = extractPartInfo(schema, table.getTTable().getSd(),
            table.getParameters(), conf, inputJobInfo);
        partInfo.setPartitionValues(new HashMap<String, String>());
        partInfoList.add(partInfo);
      }
      inputJobInfo.setPartitions(partInfoList);
    } catch (MetaException e) {
      throw new IOException("Got MetaException", e);
    } catch (NoSuchObjectException e) {
      throw new IOException("Got NoSuchObjectException", e);
    } catch (TException e) {
      throw new IOException("Got TException", e);
    } catch (HiveException e) {
      throw new IOException("Got HiveException", e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
    return inputJobInfo;
  }

  /**
   * Extract partition info.
   *
   * @param schema Table schema
   * @param sd Storage descriptor
   * @param parameters Parameters
   * @param conf Configuration
   * @param inputJobInfo Input job info
   * @return Partition info
   * @throws IOException
   */
  private static PartInfo extractPartInfo(
      HCatSchema schema, StorageDescriptor sd, Map<String, String> parameters,
      Configuration conf, InputJobInfo inputJobInfo) throws IOException {
    StorerInfo storerInfo = InternalUtil.extractStorerInfo(sd, parameters);

    Properties hcatProperties = new Properties();
    HCatStorageHandler storageHandler = HCatUtil.getStorageHandler(conf,
        storerInfo);

    // Copy the properties from storageHandler to jobProperties
    Map<String, String> jobProperties =
        HCatUtil.getInputJobProperties(storageHandler, inputJobInfo);

    for (Map.Entry<String, String> param : parameters.entrySet()) {
      hcatProperties.put(param.getKey(), param.getValue());
    }

    return new PartInfo(schema, storageHandler, sd.getLocation(),
        hcatProperties, jobProperties, inputJobInfo.getTableInfo());
  }

  /**
   * Create a new {@link HCatRecordReader}.
   *
   * @param storageHandler Storage handler
   * @param valuesNotInDataCols Values not in data columns
   * @return Record reader
   */
  public static RecordReader newHCatReader(
      HCatStorageHandler storageHandler,
      Map<String, String> valuesNotInDataCols) {
    return new HCatRecordReader(storageHandler, valuesNotInDataCols);
  }

  /**
   * Cast an {@link InputSplit} to {@link HCatSplit}.
   *
   * @param split Input split
   * @return {@link HCatSplit}
   * @throws IOException
   */
  public static HCatSplit castToHCatSplit(InputSplit split)
    throws IOException {
    return InternalUtil.castToHCatSplit(split);
  }
}
