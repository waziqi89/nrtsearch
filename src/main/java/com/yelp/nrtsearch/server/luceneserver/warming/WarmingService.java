/*
 * Copyright 2024 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.luceneserver.warming;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.luceneserver.IndexState;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarmingService {
  private static final Logger logger = LoggerFactory.getLogger(WarmingService.class);

  private static WarmingService INSTANCE;

  private boolean enable;
  private ExecutorService warmingServiceExecutor;
  private List<Warmer> warmers;

  private WarmingService() {
    this.enable = false;
    this.warmers = new ArrayList<>();
  }

  public void initialize(LuceneServerConfiguration configuration, List<Warmer> warmers) {
    this.enable =
        configuration.getIndexStartConfig().getMode() == Mode.REPLICA; // TODO: to configurable
    Collections.sort(warmers, Comparator.comparingInt(Warmer::getPriority));
    for (Warmer warmer : warmers) {
      this.warmers.add(warmer);
    }
    int parallelism = 4; // TODO: to configurable
    if (enable && parallelism > 1) {
      int numThreads = parallelism - 1;
      this.warmingServiceExecutor =
          new ThreadPoolExecutor(
              numThreads,
              numThreads,
              0,
              TimeUnit.SECONDS,
              new SynchronousQueue<>(),
              new NamedThreadFactory("warming-"),
              new ThreadPoolExecutor.CallerRunsPolicy());
    } else {
      this.warmingServiceExecutor = null;
    }
  }

  public static WarmingService getINSTANCE() {
    if (INSTANCE == null) {
      INSTANCE = new WarmingService();
    }
    return INSTANCE;
  }

  public void warm(IndexState indexState) {
    for (Warmer warmer : warmers) {
      long startTime = System.currentTimeMillis();
      List<Callable<Void>> tasks = warmer.getWarmCallableTasks(indexState);
      try {
        if (tasks != null && !tasks.isEmpty()) {
          if (warmingServiceExecutor != null) {
            try {
              // TODO: configurable timeout
              for (Future<Void> f : warmingServiceExecutor.invokeAll(tasks, 4, TimeUnit.MINUTES)) {
                f.get();
              }
            } catch (InterruptedException e) {
              logger.warn("Warmer " + warmer.getName() + " timed out.", e);
            }
          } else {
            for (Callable<Void> task : tasks) {
              task.call();
            }
          }
        }
        logger.info( "Warmer {} complete in {}ms", warmer.getName(), System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        logger.warn("warmer {} failed!", warmer.getName(), e);
        // do not block the bootstrap
      }
    }
  }

  public void close() throws InterruptedException {
    enable = false;
    warmers.clear();
    if (warmingServiceExecutor != null) {
      warmingServiceExecutor.shutdown();
      warmingServiceExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
