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
package org.apache.giraph.block_app.framework.piece.messages;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.function.Function;
import org.apache.giraph.writable.kryo.HadoopKryo;
import org.apache.hadoop.io.Writable;

/**
 * Supplier from configuration
 * @param <T> Type of object returned
 */
public interface SupplierFromConf<T>
    extends Function<ImmutableClassesGiraphConfiguration, T> {

  /**
   * Supplier from configuration, by copying given instance every time.
   *
   * @param <T> Type of object returned
   */
  public static class SupplierFromConfByCopy<T> implements SupplierFromConf<T> {
    private final T value;

    public SupplierFromConfByCopy(T value) {
      this.value = value;
    }

    @Override
    public T apply(ImmutableClassesGiraphConfiguration conf) {
      return HadoopKryo.createCopy(value);
    }
  }

  /**
   * Supplier from configuration returning DefaultMessageValueFactory instances.
   *
   * @param <M> Message type
   */
  public static class DefaultMessageFactorySupplierFromConf<M extends Writable>
      implements SupplierFromConf<MessageValueFactory<M>> {
    private final Class<M> messageClass;

    public DefaultMessageFactorySupplierFromConf(Class<M> messageClass) {
      this.messageClass = messageClass;
    }

    @Override
    public MessageValueFactory<M> apply(
        ImmutableClassesGiraphConfiguration conf) {
      return new DefaultMessageValueFactory<>(messageClass, conf);
    }
  }
}
