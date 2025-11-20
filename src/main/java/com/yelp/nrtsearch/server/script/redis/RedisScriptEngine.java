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

import com.google.common.base.Strings;
import com.yelp.nrtsearch.server.script.RuntimeScript;
import com.yelp.nrtsearch.server.script.ScriptContext;
import com.yelp.nrtsearch.server.script.ScriptEngine;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisScriptEngine implements ScriptEngine {
  public static final String LANG = "redis";
  public static final String DEFAULT_SOURCE_FOR_HOST_AND_PORT = "localhost:6379";

  /**
   * Get the script engine lang identifier for use in {@link com.yelp.nrtsearch.server.grpc.Script}.
   *
   * @return factory type identifier
   */
  @Override
  public String getLang() {
    return LANG;
  }

  /**
   * Compile the redis expression source into query level factories, which can produce script
   * factories bound to a given index and parameters.
   *
   * @param source expression source string
   * @param context script context information used to create a factory of the proper type
   * @param <T> factory type needed for this script context
   * @return compiled script factory
   * @throws IllegalArgumentException if script context is not supported or javascript compile fails
   */
  @Override
  public <T> T compile(String source, ScriptContext<T> context) {
    if (!context.equals(RuntimeScript.CONTEXT)) {
      throw new IllegalArgumentException("Unsupported script context: " + context.name);
    }
    String hostPort = DEFAULT_SOURCE_FOR_HOST_AND_PORT;
    if (!Strings.isNullOrEmpty(source)) {
        hostPort = source;
    }
    JedisPool jedisPool;
    try {
        String[] hostPortArr =  hostPort.split(":");
      jedisPool = new JedisPool(new JedisPoolConfig(), hostPortArr[0], Integer.parseInt(hostPortArr[1]));
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage());
    }

    RedisRuntimeScript.RedisFactory redisFactory = new RedisRuntimeScript.RedisFactory(jedisPool);

    return context.factoryClazz.cast(redisFactory);
  }
}
