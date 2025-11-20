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

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.TextBaseFieldDef;
import java.util.Map;

public class RedisScriptConfigs {
  public enum RedisResponseType {
    STRING,
    LIST_OF_DOUBLE,
  }

  public static final String REDIS_STATIC_KEY_PARAM_MAME = "redisStaticKey";
  public static final String REDIS_KEY_FIELD_NAME_PARAM_NAME = "redisKeyField";
  public static final String REDIS_RESPONSE_TYPE_FIELD_NAME = "redisResponseType";

  private final String staticRedisKey;
  private final String redisKeyField;

  public String getStaticRedisKey() {
    return staticRedisKey;
  }

  public String getRedisKeyField() {
    return redisKeyField;
  }

  public RedisResponseType getRedisResponseType() {
    return redisResponseType;
  }

  private final RedisResponseType redisResponseType;

  private RedisScriptConfigs(
      String staticRedisKey, String redisKeyField, RedisResponseType redisResponseType) {
    this.staticRedisKey = staticRedisKey;
    this.redisKeyField = redisKeyField;
    this.redisResponseType = redisResponseType;
  }

  public static RedisScriptConfigs fromParams(Map<String, Object> params) {
    String staticRedisKey = params.getOrDefault(REDIS_STATIC_KEY_PARAM_MAME, "").toString();
    String redisKeyField = params.getOrDefault(REDIS_KEY_FIELD_NAME_PARAM_NAME, "").toString();
    RedisResponseType redisResponseType =
        RedisResponseType.valueOf(
            params.getOrDefault(REDIS_RESPONSE_TYPE_FIELD_NAME, "STRING").toString());
    return new RedisScriptConfigs(staticRedisKey, redisKeyField, redisResponseType);
  }

  public void validate(DocLookup docLookup) {
    if (!staticRedisKey.isEmpty()) {
      return;
    } else if (redisKeyField.isEmpty()) {
      throw new IllegalArgumentException(
          "One of staticRedisKey or redisKeyField must be present in the params");
    }

    FieldDef keyFieldDef = docLookup.getFieldDef(redisKeyField);
    if (keyFieldDef == null) {
      throw new IllegalArgumentException("redisKeyField " + redisKeyField + " does not exist");
    } else if (!(keyFieldDef instanceof TextBaseFieldDef)) {
      throw new IllegalArgumentException("redisKeyField " + redisKeyField + "is not a text field");
    }
    TextBaseFieldDef keyTextFieldDef = (TextBaseFieldDef) keyFieldDef;
    if (keyTextFieldDef.isStored() && keyTextFieldDef.isMultiValue()) {
      throw new IllegalArgumentException(
          "redisKeyField " + redisKeyField + "is not stored or is multivalue");
    }
  }
}
