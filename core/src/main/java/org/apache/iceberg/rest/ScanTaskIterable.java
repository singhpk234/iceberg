/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanTaskIterable implements CloseableIterable<FileScanTask> {

  private static final Logger LOG = LoggerFactory.getLogger(ScanTaskIterable.class);
  private static final int DEFAULT_TASK_QUEUE_CAPACITY = 1000;
  private static final long QUEUE_POLL_TIMEOUT_MS = 100;
  private static final int WORKER_POOL_SIZE = Math.max(1, ThreadPools.WORKER_THREAD_POOL_SIZE / 4);
  private final BlockingQueue<FileScanTask> taskQueue;
  private final ConcurrentLinkedQueue<FileScanTask> initialFileScanTasks;
  private final ConcurrentLinkedQueue<String> planTasks;
  private final AtomicInteger activeWorkers = new AtomicInteger(0);
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final AtomicBoolean drainingInitialTasks = new AtomicBoolean(false);
  private final ExecutorService executorService;
  private final RESTClient client;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Map<String, String> headers;
  private final ParserContext parserContext;

  ScanTaskIterable(
      List<String> initialPlanTasks,
      List<FileScanTask> initialFileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Map<String, String> headers,
      ExecutorService executorService,
      ParserContext parserContext) {

    this.taskQueue = new LinkedBlockingQueue<>(DEFAULT_TASK_QUEUE_CAPACITY);
    this.planTasks = new ConcurrentLinkedQueue<>();
    this.initialFileScanTasks = new ConcurrentLinkedQueue<>(initialFileScanTasks);

    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
    this.executorService = executorService;
    this.parserContext = parserContext;
    if (initialPlanTasks != null && !initialPlanTasks.isEmpty()) {
      planTasks.addAll(initialPlanTasks);
    } else if (initialFileScanTasks.isEmpty()) {
      // No tasks to produce
      return;
    }

    startInitialWorkers();
  }

  private void startInitialWorkers() {
    for (int i = 0; i < WORKER_POOL_SIZE; i++) {
      executorService.execute(new PlanTaskWorker());
    }
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    return new ScanTaskIterator();
  }

  @Override
  public void close() throws IOException {}

  /**
   * Worker that populates the taskQueue for consumers from the initial tasks or plan tasks.
   *
   * <p>Workers expand plan tasks into file scan tasks, may enqueue additional plan tasks for
   * further processing. One worker is responsible for seeding the taskQueue with the initial scan
   * tasks. When the last active worker finishes and unprocessed plan tasks remain, a new worker is
   * scheduled to continue planning until shutdown is requested.
   */
  private class PlanTaskWorker implements Runnable {

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void run() {
      activeWorkers.incrementAndGet();

      try {
        while (!shutdown.get() && !Thread.currentThread().isInterrupted()) {
          // First lucky plan worker is responsible for draining the initial tasks into the
          // taskQueue
          if (drainingInitialTasks.compareAndSet(false, true)) {
            while (!initialFileScanTasks.isEmpty() && !shutdown.get()) {
              FileScanTask initialFileScanTask = initialFileScanTasks.poll();
              if (initialFileScanTask != null) {
                taskQueue.put(initialFileScanTask);
              }
            }
          }

          String planTask = planTasks.poll();
          if (planTask == null) {
            return;
          }

          processPlanTask(planTask);
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException("Worker failed processing planTask", e);
      } finally {
        int remaining = activeWorkers.decrementAndGet();
        if (remaining == 0 && !planTasks.isEmpty() && !shutdown.get()) {
          executorService.execute(new PlanTaskWorker());
        }
      }
    }

    private void processPlanTask(String planTask) throws InterruptedException {
      FetchScanTasksResponse response = fetchScanTasks(planTask);
      // Add file scan tasks first so that consumers can start consuming as soon as they are ready
      if (response.fileScanTasks() != null) {
        for (FileScanTask task : response.fileScanTasks()) {
          if (!shutdown.get()) {
            taskQueue.put(task);
          }
        }
      }

      if (!shutdown.get() && response.planTasks() != null) {
        planTasks.addAll(response.planTasks());
      }
    }

    private FetchScanTasksResponse fetchScanTasks(String planTask) {
      FetchScanTasksRequest request = new FetchScanTasksRequest(planTask);

      return client.post(
          resourcePaths.fetchScanTasks(tableIdentifier),
          request,
          FetchScanTasksResponse.class,
          headers,
          ErrorHandlers.defaultErrorHandler(),
          stringStringMap -> {},
          parserContext);
    }
  }

  /** Iterator which uses taskQueue as the source of truth of file scan tasks */
  private class ScanTaskIterator implements CloseableIterator<FileScanTask> {
    private FileScanTask nextTask = null;

    @Override
    public boolean hasNext() {
      if (nextTask != null) {
        return true;
      }

      while (true) {
        if (isDone()) {
          return false;
        }

        try {
          nextTask = taskQueue.poll(QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (nextTask != null) {
            return true;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    @Override
    public FileScanTask next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more scan tasks available");
      }

      FileScanTask result = nextTask;
      nextTask = null;
      return result;
    }

    @Override
    public void close() {
      shutdown.set(true);
      LOG.info(
          "ScanTasksIterator is closing. Clearing {} queued tasks and {} plan tasks.",
          taskQueue.size(),
          planTasks.size());
      taskQueue.clear();
      planTasks.clear();
    }

    private boolean isDone() {
      return taskQueue.isEmpty()
          && planTasks.isEmpty()
          && activeWorkers.get() == 0
          && initialFileScanTasks.isEmpty();
    }
  }
}
