/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.handler;

import com.yelp.nrtsearch.server.grpc.IndexName;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.SearcherVersion;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.nrt.NRTPrimaryNode;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.DirectoryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteNRTPointHandler extends Handler<IndexName, SearcherVersion> {
  private static final Logger logger = LoggerFactory.getLogger(WriteNRTPointHandler.class);

  public WriteNRTPointHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public SearcherVersion handle(IndexName indexNameRequest) throws Exception {
    IndexStateManager indexStateManager = getIndexStateManager(indexNameRequest.getIndexName());
    String indexId = indexStateManager.getIndexId();
    IndexState indexState = indexStateManager.getCurrent();
    SearcherVersion reply = handle(indexState, indexId);
    logger.debug("WriteNRTPointHandler returned version {}", reply.getVersion());
    return reply;
  }

  private SearcherVersion handle(IndexState indexState, String indexId) {
    final ShardState shardState = indexState.getShard(0);

    if (!shardState.isPrimary()) {
      throw new IllegalArgumentException(
          "index \"" + indexState.getName() + "\" is either not started or is not a primary index");
    }
    SearcherVersion.Builder searchverVersionBuilder = SearcherVersion.newBuilder();
    try {
      if (shardState.nrtPrimaryNode.flushAndRefresh()) {
        // Something did get flushed (there were indexing ops since the last flush):

        // nocommit: we used to notify caller of the version, before trying to push to replicas, in
        // case we crash after flushing but
        // before notifying all replicas, at which point we have a newer version index than client
        // knew about?
        long version = shardState.nrtPrimaryNode.getCopyStateVersion();
        Collection<NRTPrimaryNode.ReplicaDetails> replicasInfos =
            shardState.nrtPrimaryNode.getNodesInfo();
        shardState.nrtPrimaryNode.message(
            "send flushed version=" + version + " replica count " + replicasInfos.size());

        // Notify current replicas:
        Iterator<NRTPrimaryNode.ReplicaDetails> it = replicasInfos.iterator();
        while (it.hasNext()) {
          NRTPrimaryNode.ReplicaDetails replicaDetails = it.next();
          String nodeName = replicaDetails.getNodeName();
          ReplicationServerClient currentReplicaServerClient =
              replicaDetails.getReplicationServerClient();
          try {
            // TODO: we should use multicast to broadcast files out to replicas
            // TODO: ... replicas could copy from one another instead of just primary
            // TODO: we could also prioritize one replica at a time?
            currentReplicaServerClient.newNRTPoint(
                indexState.getName(), indexId, shardState.nrtPrimaryNode.getPrimaryGen(), version);
          } catch (StatusRuntimeException e) {
            Status status = e.getStatus();
            if (status.getCode().equals(Status.UNAVAILABLE.getCode())) {
              logger.info(
                  "NRTPRimaryNode: sendNRTPoint, lost connection to nodeName: "
                      + nodeName
                      + " host: "
                      + replicaDetails.getHostPort().getHostName()
                      + " port: "
                      + replicaDetails.getHostPort().getPort());
              it.remove();
            }
          } catch (Exception e) {
            shardState.nrtPrimaryNode.message(
                "top: failed to connect "
                    + nodeName
                    + " for newNRTPoint; skipping: "
                    + e.getMessage());
            logger.info(
                "top: failed to connect "
                    + nodeName
                    + " for newNRTPoint; skipping: "
                    + e.getMessage());
          }
        }
        return searchverVersionBuilder.setVersion(version).setDidRefresh(true).build();
      } else {
        SearcherTaxonomyManager.SearcherAndTaxonomy s = shardState.acquire();
        try {
          long version = ((DirectoryReader) s.searcher().getIndexReader()).getVersion();
          return searchverVersionBuilder.setVersion(version).setDidRefresh(false).build();
        } finally {
          shardState.release(s);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
