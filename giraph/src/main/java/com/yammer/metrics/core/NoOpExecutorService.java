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

package com.yammer.metrics.core;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An executor service that does nothing. Used with empty metrics so that no
 * threads are created wasting time / space.
 */
public class NoOpExecutorService implements ScheduledExecutorService {
  @Override
  public ScheduledFuture<?> schedule(Runnable runnable, long l,
                                     TimeUnit timeUnit) {
    return null;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> vCallable, long l,
                                         TimeUnit timeUnit) {
    return null;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
    Runnable runnable, long l, long l1, TimeUnit timeUnit
  ) {
    return null;
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
    Runnable runnable, long l, long l1, TimeUnit timeUnit
  ) {
    return null;
  }

  @Override
  public void shutdown() {
  }

  @Override
  public List<Runnable> shutdownNow() {
    return null;
  }

  @Override
  public boolean isShutdown() {
    return false;
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit)
    throws InterruptedException {
    return false;
  }

  @Override
  public <T> Future<T> submit(Callable<T> tCallable) {
    return null;
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T t) {
    return null;
  }

  @Override
  public Future<?> submit(Runnable runnable) {
    return null;
  }

  @Override
  public <T> List<Future<T>> invokeAll(
    Collection<? extends Callable<T>> callables)
    throws InterruptedException {
    return null;
  }

  @Override
  public <T> List<Future<T>> invokeAll(
    Collection<? extends Callable<T>> callables, long l, TimeUnit timeUnit
  ) throws InterruptedException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> callables)
    throws InterruptedException, ExecutionException {
    return null;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> callables, long l,
                         TimeUnit timeUnit)
    throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  @Override
  public void execute(Runnable runnable) {
  }
}
