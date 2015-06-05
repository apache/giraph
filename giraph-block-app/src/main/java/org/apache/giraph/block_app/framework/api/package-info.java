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
/**
 * Interfaces representing full API to the underlying graph processing system.
 *
 * Framework implementation is fully contained within package
 * org.apache.giraph.block_app.framework, given implementation of interfaces
 * defined here.
 *
 * He have two such implementations:
 * - one relying on Giraph, distributed graph processing system,
 *   connecting all methods to it's internals
 * - one having a fully contained local implementation, executing applications
 *   on a single machine. Avoiding overheads of Giraph being distributed,
 *   it allows very efficient evaluation on small graphs, especially useful for
 *   fast unit tests.
 *
 * You could potentially use a different graph processing system, to execute
 * any Block Application, by implementing these interfaces.
 */
package org.apache.giraph.block_app.framework.api;
