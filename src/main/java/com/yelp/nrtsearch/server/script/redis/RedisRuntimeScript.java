/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.script.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.script.RuntimeScript;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.LeafReaderContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisRuntimeScript extends RuntimeScript {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Jedis jedis;
  private final RedisScriptConfigs configs;
  private final ConcurrentHashMap<String, Object> requestCache;

  private RedisRuntimeScript(
      Map<String, Object> params,
      DocLookup docLookup,
      LeafReaderContext leafContext,
      Jedis jedis,
      RedisScriptConfigs configs,
      ConcurrentHashMap<String, Object> requestCache) {
    super(params, docLookup, leafContext);
    this.jedis = jedis;
    this.configs = configs;
    this.requestCache = requestCache;
  }

  private Object computeAndCast(String key) {
    // This is guaranteed to a nullable string
    String strValue = jedis.get(key);
    try {
      return switch (configs.getRedisResponseType()) {
        case RedisScriptConfigs.RedisResponseType.LIST_OF_DOUBLE ->
            OBJECT_MAPPER.readValue(strValue, new TypeReference<List<Double>>() {});
        default -> strValue;
      };
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public Object execute() {

    final String redisKeyValueInString;
    if (!configs.getStaticRedisKey().isEmpty()) {
      redisKeyValueInString = configs.getStaticRedisKey();
    } else {
      LoadedDocValues<?> redisKeyValue = getDoc().get(configs.getRedisKeyField());
      if (redisKeyValue instanceof LoadedDocValues.SingleString) {
        redisKeyValueInString = ((LoadedDocValues.SingleString) redisKeyValue).getValue();
      } else {
        // This shall not happen as it's checked earlier
        throw new IllegalArgumentException("The key field must be single text value");
      }
    }

    return requestCache.computeIfAbsent(redisKeyValueInString, k -> computeAndCast(k));
  }

  public static class RedisFactory implements RuntimeScript.Factory {

    private final JedisPool jedisPool;
    private final ConcurrentHashMap<String, Object> requestCache;

    public RedisFactory(JedisPool jedisPool) {
      this.jedisPool = jedisPool;
      this.requestCache = new ConcurrentHashMap<>();
    }

    @Override
    public RuntimeScript.SegmentFactory newFactory(
        Map<String, Object> params, DocLookup docLookup) {
      RedisScriptConfigs configs = RedisScriptConfigs.fromParams(params);
      configs.validate(docLookup);

      return new RedisSegmentFactory(jedisPool, configs, requestCache, docLookup, params);
    }
  }

  public static class RedisSegmentFactory implements RuntimeScript.SegmentFactory {
    private final JedisPool jedisPool;
    private final RedisScriptConfigs configs;
    private final ConcurrentHashMap<String, Object> requestCache;
    private final DocLookup docLookup;
    private final Map<String, Object> params;

    private RedisSegmentFactory(
        JedisPool jedisPool,
        RedisScriptConfigs configs,
        ConcurrentHashMap<String, Object> requestCache,
        DocLookup docLookup,
        Map<String, Object> params) {
      this.jedisPool = jedisPool;
      this.configs = configs;
      this.requestCache = requestCache;
      this.docLookup = docLookup;
      this.params = params;
    }

    @Override
    public RuntimeScript newInstance(LeafReaderContext leafContext) throws IOException {
      return new RedisRuntimeScript(
          params, docLookup, leafContext, jedisPool.getResource(), configs, requestCache);
    }
  }
}
