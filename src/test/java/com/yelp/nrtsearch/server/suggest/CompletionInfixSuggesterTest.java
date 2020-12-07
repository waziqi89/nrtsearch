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
package com.yelp.nrtsearch.server.suggest;

import com.google.protobuf.ByteString;
import com.yelp.nrtsearch.server.grpc.NrtsearchIndex;
import com.yelp.nrtsearch.server.luceneserver.suggest.CompletionInfixSuggester;
import com.yelp.nrtsearch.server.luceneserver.suggest.FromProtobufFileSuggestItemIterator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompletionInfixSuggesterTest extends LuceneTestCase {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private CompletionInfixSuggester suggester;

  @Before
  public void before() throws Exception {
    Directory dir = newDirectory();
    FromProtobufFileSuggestItemIterator iter = createIterator();
    Analyzer analyzer = new StandardAnalyzer();
    suggester = new CompletionInfixSuggester(dir, analyzer, analyzer);
    suggester.build(iter);
  }

  @After
  public void after() throws Exception {
    suggester.close();
  }

  @Test
  public void testCompletionSuggestionWithoutContext() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of(), 2);

    assertNotNull(actualResults);
    assertEquals(2, actualResults.size());
    assertEquals("1", actualResults.get(0).key);
    assertEquals(4, actualResults.get(0).value);
    assertEquals("2", actualResults.get(1).key);
    assertEquals(2, actualResults.get(1).value);
  }

  @Test
  public void testSuggesterWithOneContext() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of("9q9hxb"), 2);

    assertNotNull(actualResults);
    assertEquals(1, actualResults.size());
    assertEquals("2", actualResults.get(0).key);
    assertEquals(2, actualResults.get(0).value);
  }

  @Test
  public void testSuggesterWithOneContextWithNoMatch() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of("abcde"), 2);

    assertNotNull(actualResults);
    assertEquals(0, actualResults.size());
  }

  @Test
  public void testSuggesterWithOneContextWithMultipleMatches() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of("9q9hf"), 2);

    assertNotNull(actualResults);
    assertEquals(2, actualResults.size());
    assertEquals("1", actualResults.get(0).key);
    assertEquals(4, actualResults.get(0).value);
    assertEquals("0", actualResults.get(1).key);
    assertEquals(1, actualResults.get(1).value);
  }

  @Test
  public void testSuggesterWithMultipleContexts() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of("9q9hx", "9q9hf"), 2);

    assertNotNull(actualResults);
    assertEquals(2, actualResults.size());
    assertEquals("1", actualResults.get(0).key);
    assertEquals(4, actualResults.get(0).value);
    assertEquals("2", actualResults.get(1).key);
    assertEquals(2, actualResults.get(1).value);
  }

  @Test
  public void testSuggesterWithUpdatedDocs() throws IOException {
    List<LookupResult> actualResults;
    actualResults = lookupHelper(suggester, "hom", Set.of(), 20);
    assertEquals(3, actualResults.size());

    suggester.update(
        new BytesRef("2"),
        Set.of(new BytesRef("home decoration"), new BytesRef("decoration")),
        Set.of(new BytesRef("9q9hfe"), new BytesRef("9q9hf")),
        10L,
        new BytesRef("new payload"));
    suggester.update(
        new BytesRef("10"),
        Set.of(new BytesRef("new home"), new BytesRef("home")),
        Set.of(new BytesRef("9q9hfe"), new BytesRef("9q9hf")),
        20L,
        new BytesRef("new payload"));

    actualResults = lookupHelper(suggester, "hom", Set.of(), 20);
    assertNotNull(actualResults);
    assertEquals(3, actualResults.size());

    suggester.refresh();

    actualResults = lookupHelper(suggester, "hom", Set.of(), 20);
    assertNotNull(actualResults);
    assertEquals(4, actualResults.size());
    assertEquals("10", actualResults.get(0).key);
    assertEquals(20, actualResults.get(0).value);
    assertEquals("2", actualResults.get(1).key);
    assertEquals(10, actualResults.get(1).value);
    assertEquals("1", actualResults.get(2).key);
    assertEquals(4, actualResults.get(2).value);
    assertEquals("0", actualResults.get(3).key);
    assertEquals(1, actualResults.get(3).value);
  }

  @Test
  public void testSuggesterWithDuplicatedPrefixDoc() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "sha", Set.of("9q9hf"), 2);
    assertNotNull(actualResults);
    assertEquals(1, actualResults.size());
    assertEquals(3, actualResults.get(0).value);
    assertEquals("4", actualResults.get(0).key);
  }

  @Test(expected = RuntimeException.class)
  public void testSuggesterLookupWithoutValidIndexBuild() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    CompletionInfixSuggester testSuggester = new CompletionInfixSuggester(dir, analyzer, analyzer);
    try {
      lookupHelper(testSuggester, "sha", Set.of("9q9hf"), 2);
    } finally {
      testSuggester.close();
    }
  }

  private List<LookupResult> lookupHelper(
      Lookup suggester, String key, Set<String> contexts, int count) throws IOException {
    Set<BytesRef> contextSet = new HashSet<>();
    for (String context : contexts) {
      contextSet.add(new BytesRef(context));
    }

    return suggester.lookup(key, contextSet, true, count);
  }

  private FromProtobufFileSuggestItemIterator createIterator() throws Exception {
    File outputFile =
        Paths.get(folder.newFolder("nrtsearch_file").toPath().toString(), "fuzzy_suggest_item.file")
            .toFile();

    List<List<String>> searchTextsList =
        List.of(
            List.of("home depot", "depot"),
            List.of("lowe's home improvement", "home improvement", "improvement"),
            List.of("home decoration", "decoration"),
            List.of("gary danko restaurant", "danko restaurant", "restaurant"),
            List.of("shack shack", "shack"));
    List<String> payloads = List.of("payload1", "payload2", "payload3", "payload4", "payload5");
    List<List<String>> contextsList =
        List.of(
            List.of("9q9hfe", "9q9hf"),
            List.of("9q9hfe", "9q9hf"),
            List.of("9q9hxb", "9q9hx"),
            List.of("9q9hbw", "9q9hb"),
            List.of("9q9hfe", "9q9hf"));
    List<Long> scores = List.of(1L, 4L, 2L, 3L, 3L);

    try (FileOutputStream protoFile = new FileOutputStream(outputFile)) {
      for (int i = 0; i < searchTextsList.size(); i++) {
        NrtsearchIndex.newBuilder()
            .setUniqueId(i)
            .addAllSearchTexts(searchTextsList.get(i))
            .setScore(scores.get(i))
            .setPayload(ByteString.copyFrom(payloads.get(i).getBytes()))
            .addAllContexts(contextsList.get(i))
            .build()
            .writeDelimitedTo(protoFile);
      }
    }
    return new FromProtobufFileSuggestItemIterator(outputFile, true, true);
  }
}