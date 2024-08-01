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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.TextDocValuesType;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import org.apache.lucene.index.DocValuesType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextFieldDefTest {

  @BeforeClass
  public static void init() {
    String configStr = "node: node1";
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
    SimilarityCreator.initialize(configuration, Collections.emptyList());
  }

  private TextFieldDef createFieldDef(Field field) {
    return new TextFieldDef("test_field", field);
  }

  @Test
  public void testDocValueType_none() {
    TextFieldDef fieldDef = createFieldDef(Field.newBuilder().setStoreDocValues(false).build());
    assertEquals(DocValuesType.NONE, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_default() {
    TextFieldDef fieldDef = createFieldDef(Field.newBuilder().setStoreDocValues(true).build());
    assertEquals(DocValuesType.SORTED, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_sorted() {
    TextFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setStoreDocValues(true)
                .setTextDocValuesType(TextDocValuesType.TEXT_DOC_VALUES_TYPE_SORTED)
                .build());
    assertEquals(DocValuesType.SORTED, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_binary() {
    TextFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setStoreDocValues(true)
                .setTextDocValuesType(TextDocValuesType.TEXT_DOC_VALUES_TYPE_BINARY)
                .build());
    assertEquals(DocValuesType.BINARY, fieldDef.getDocValuesType());
  }
}
