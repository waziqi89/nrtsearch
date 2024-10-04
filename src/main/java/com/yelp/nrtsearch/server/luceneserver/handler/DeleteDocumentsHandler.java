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
package com.yelp.nrtsearch.server.luceneserver.handler;

import com.google.protobuf.ProtocolStringList;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentResponse;
import com.yelp.nrtsearch.server.luceneserver.index.IndexState;
import com.yelp.nrtsearch.server.luceneserver.index.ShardState;
import com.yelp.nrtsearch.server.luceneserver.state.GlobalState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDocumentsHandler extends Handler<AddDocumentRequest, AddDocumentResponse> {
  private static final Logger logger =
      LoggerFactory.getLogger(DeleteDocumentsHandler.class.getName());

  public DeleteDocumentsHandler(GlobalState globalState) {
    super(globalState);
  }

  @Override
  public void handle(
      AddDocumentRequest addDocumentRequest, StreamObserver<AddDocumentResponse> responseObserver) {
    try {
      IndexState indexState = getGlobalState().getIndex(addDocumentRequest.getIndexName());
      AddDocumentResponse reply = handleInternal(indexState, addDocumentRequest);
      logger.debug("DeleteDocumentsHandler returned {}", reply);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error(
          "error while trying to delete documents for index {}",
          addDocumentRequest.getIndexName(),
          e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  "error while trying to delete documents for index: "
                      + addDocumentRequest.getIndexName())
              .augmentDescription(e.getMessage())
              .asRuntimeException());
    }
  }

  private AddDocumentResponse handleInternal(
      IndexState indexState, AddDocumentRequest addDocumentRequest)
      throws DeleteDocumentsHandlerException {
    final ShardState shardState = indexState.getShard(0);
    indexState.verifyStarted();

    Map<String, AddDocumentRequest.MultiValuedField> fields = addDocumentRequest.getFieldsMap();
    List<Term> terms = new ArrayList<>();
    for (Map.Entry<String, AddDocumentRequest.MultiValuedField> entry : fields.entrySet()) {
      String fieldName = entry.getKey();
      AddDocumentRequest.MultiValuedField multiValuedField = entry.getValue();
      ProtocolStringList fieldValues = multiValuedField.getValueList();
      for (String fieldValue : fieldValues) {
        // TODO: how to allow arbitrary binary keys?  how to
        // pass binary data via json...?  byte array?
        terms.add(new Term(fieldName, fieldValue));
      }
    }
    try {
      shardState.writer.deleteDocuments(terms.stream().toArray(Term[]::new));
    } catch (IOException e) {
      logger.warn(
          "ThreadId: {}, writer.deleteDocuments failed",
          Thread.currentThread().getName() + Thread.currentThread().getId());
      throw new DeleteDocumentsHandlerException(e);
    }
    long genId = shardState.writer.getMaxCompletedSequenceNumber();
    return AddDocumentResponse.newBuilder()
        .setGenId(String.valueOf(genId))
        .setPrimaryId(indexState.getGlobalState().getEphemeralId())
        .build();
  }

  public static class DeleteDocumentsHandlerException extends Handler.HandlerException {

    public DeleteDocumentsHandlerException(Throwable err) {
      super(err);
    }
  }
}