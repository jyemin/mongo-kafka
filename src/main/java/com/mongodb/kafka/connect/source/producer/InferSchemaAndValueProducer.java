/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.source.producer;

import org.apache.kafka.connect.data.SchemaAndValue;

import org.bson.BsonDocument;
import org.bson.json.JsonWriterSettings;

import com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema;
import com.mongodb.kafka.connect.source.schema.BsonDocumentToSchemaOneDotEight;
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue;

final class InferSchemaAndValueProducer implements SchemaAndValueProducer {
  private final BsonValueToSchemaAndValue bsonValueToSchemaAndValue;
  private final boolean combineCompatibleArraySchemas;

  InferSchemaAndValueProducer(
      final JsonWriterSettings jsonWriterSettings, final boolean combineCompatibleArraySchemas) {
    bsonValueToSchemaAndValue = new BsonValueToSchemaAndValue(jsonWriterSettings);
    this.combineCompatibleArraySchemas = combineCompatibleArraySchemas;
  }

  @Override
  public SchemaAndValue get(final BsonDocument changeStreamDocument) {
    if (combineCompatibleArraySchemas) {
      return bsonValueToSchemaAndValue.toSchemaAndValue(
          BsonDocumentToSchema.inferDocumentSchema(changeStreamDocument), changeStreamDocument);
    } else {
      return bsonValueToSchemaAndValue.toSchemaAndValue(
          BsonDocumentToSchemaOneDotEight.inferDocumentSchema(changeStreamDocument),
          changeStreamDocument);
    }
  }
}
