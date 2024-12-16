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

import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentPreloadWarmer implements Warmer {
  private static final Logger logger = LoggerFactory.getLogger(SegmentPreloadWarmer.class);

  private static final int DEFAULT_PRIORITY = 10;
  private static final SimpleMergedSegmentWarmer SIMPLE_MERGED_SEGMENT_WARMER =
      new SimpleMergedSegmentWarmer(InfoStream.NO_OUTPUT);

  public SegmentPreloadWarmer() {}

  @Override
  public int getPriority(){ return DEFAULT_PRIORITY;}

  @Override
  public List<Callable<Void>> getWarmCallableTasks(IndexState indexState) {
    try {
      return warmFromSegmentFiles(indexState);
    } catch (IOException | InterruptedException e) {
      logger.info("failed getting leaves " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private List<Callable<Void>> warmFromSegmentFiles(IndexState indexState)
      throws IOException, InterruptedException {
    ShardState state = indexState.getShard(0);
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    try {
      s = state.acquire();
      List<Callable<Void>> list = new ArrayList<>();
      for (LeafReaderContext leafReaderContext : s.searcher.getIndexReader().leaves()) {
        list.add(
            () -> {
              long startTime = System.currentTimeMillis();
              try {
                SIMPLE_MERGED_SEGMENT_WARMER.warm(leafReaderContext.reader());
                return null;
              } finally {
                logger.debug(
                    "{} warmer takes {}ms to load {}",
                    this.getClass().getName(),
                    System.currentTimeMillis() - startTime,
                    leafReaderContext.id());
              }
            });
      }
      return list;
    } finally {
      if (s != null) {
        state.release(s);
      }
    }
  }
}
