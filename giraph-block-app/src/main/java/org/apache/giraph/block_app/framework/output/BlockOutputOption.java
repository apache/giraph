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
package org.apache.giraph.block_app.framework.output;

import org.apache.giraph.block_app.framework.api.BlockOutputApi;
import org.apache.giraph.conf.GiraphConfiguration;

/**
 * Block output option, with apis to use from application code
 *
 * @param <OD> Output description type
 * @param <OW> Output writer type
 */
public class BlockOutputOption<OD extends BlockOutputDesc<OW>,
    OW extends BlockOutputWriter> {
  private final String key;

  public BlockOutputOption(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }

  public void register(OD outputDesc, GiraphConfiguration conf) {
    BlockOutputFormat.addOutputDesc(outputDesc, key, conf);
  }

  public OD getOutputDesc(BlockOutputApi outputApi) {
    return outputApi.<OW, OD>getOutputDesc(key);
  }

  public OW getWriter(BlockOutputApi outputApi) {
    return outputApi.getWriter(key);
  }
}
