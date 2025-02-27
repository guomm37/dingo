/*
 * Copyright 2021 DataCanvas
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

package io.dingodb.store.proxy.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.sdk.service.entity.common.DocumentIndexParameter;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.MetricType;
import io.dingodb.sdk.service.entity.common.ScalarField;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;
import io.dingodb.sdk.service.entity.common.ScalarIndexParameter;
import io.dingodb.sdk.service.entity.common.ScalarIndexType;
import io.dingodb.sdk.service.entity.common.ScalarSchema;
import io.dingodb.sdk.service.entity.common.ScalarSchemaItem;
import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.ValueType;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.DiskannParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.FlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.HnswParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfFlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfPqParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexType;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.store.proxy.mapper.Mapper.JSON;

public interface IndexMapper {

    default void setIndex(IndexTable.IndexTableBuilder builder, IndexParameter indexParameter) {
        if (indexParameter == null) {
            builder.indexType(io.dingodb.meta.entity.IndexType.SCALAR);
            return;
        }
        if (indexParameter.getIndexType() == IndexType.INDEX_TYPE_VECTOR) {
            switch (indexParameter.getVectorIndexParameter().getVectorIndexType()) {
                case VECTOR_INDEX_TYPE_FLAT:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_FLAT);
                    break;
                case VECTOR_INDEX_TYPE_IVF_FLAT:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_IVF_FLAT);
                    break;
                case VECTOR_INDEX_TYPE_IVF_PQ:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_IVF_PQ);
                    break;
                case VECTOR_INDEX_TYPE_HNSW:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_HNSW);
                    break;
                case VECTOR_INDEX_TYPE_DISKANN:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_DISKANN);
                    break;
                case VECTOR_INDEX_TYPE_BRUTEFORCE:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_BRUTEFORCE);
                    break;
                default:
                    throw new IllegalStateException(
                        "Unexpected value: " + indexParameter.getVectorIndexParameter().getVectorIndexType()
                    );
            }
            builder.properties(toMap(
                Optional.mapOrNull(indexParameter.getVectorIndexParameter(),
                    VectorIndexParameter::getVectorIndexParameter)
            ));
        } else if (indexParameter.getIndexType() == IndexType.INDEX_TYPE_DOCUMENT) {
            builder.indexType(io.dingodb.meta.entity.IndexType.DOCUMENT);
        } else {
            builder.unique(indexParameter.getScalarIndexParameter().isUnique());
            builder.indexType(io.dingodb.meta.entity.IndexType.SCALAR);
        }
        builder.originKeyList(indexParameter.getOriginKeys());
        builder.originWithKeyList(indexParameter.getOriginWithKeys());
    }

    default Properties toMap(Object target) {
        if (target == null) {
            return new Properties();
        }
        try {
            return JSON.convertValue(target, Properties.class);
        } catch (Exception e) {
            return new Properties();
        }
    }

    default void resetIndexParameter(
        TableDefinition indexDefinition, io.dingodb.common.table.IndexDefinition indexDef
    ) {
        Map<String, String> properties = indexDefinition.getProperties();
        String indexType = properties.get("indexType");
        if (indexType.equals("scalar")) {
            indexDefinition.setIndexParameter(
                IndexParameter.builder()
                    .indexType(IndexType.INDEX_TYPE_SCALAR)
                    .scalarIndexParameter(ScalarIndexParameter.builder()
                        .isUnique(indexDef.isUnique())
                        .scalarIndexType(ScalarIndexType.SCALAR_INDEX_TYPE_LSM)
                        .build()
                    ).originKeys(indexDef.getOriginKeyList())
                    .originWithKeys(indexDef.getOriginWithKeyList())
                    .build()
            );
        } else if (indexType.equals("document")) {
            DocumentIndexParameter documentIndexParameter;
            List<ColumnDefinition> columns = indexDefinition.getColumns();
            String json = properties.get("text_fields");
            if (json == null) {
                throw new RuntimeException("argument \"text_fields\" is null");
            }
            try {
                JsonNode jsonNode = JSON.readTree(json);
                Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
                List<ScalarSchemaItem> scalarSchemaItems = new ArrayList<>();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> next = fields.next();
                    if (!columns.stream().map(ColumnDefinition::getName).collect(Collectors.toList())
                        .contains(next.getKey().toUpperCase())) {
                        throw new RuntimeException("The field: [" + next.getKey()
                            + "] does not exist in the document index");
                    }
                    json = json.replace(next.getKey(), next.getKey().toUpperCase());

                    JsonNode tokenizer = next.getValue().get("tokenizer");
                    if (tokenizer == null) {
                        throw new RuntimeException("Tokenizer parameters not found");
                    }
                    String type = next.getValue().get("tokenizer").get("type").asText();
                    ColumnDefinition columnDefinition = columns.stream()
                        .filter(c -> c.getName().equalsIgnoreCase(next.getKey())).findFirst().orElse(null);
                    if (columnDefinition != null
                        && !checkType(type.toUpperCase(), columnDefinition.getSqlType().toUpperCase())) {
                        throw new RuntimeException("Table field type: " + columnDefinition.getSqlType()
                            + " and Tokenizer type : " + type + " do not match");
                    }
                    ScalarFieldType scalarFieldType;
                    switch (type.toLowerCase()) {
                        case "default":
                        case "raw":
                        case "simple":
                        case "stem":
                        case "whitespace":
                        case "ngram":
                        case "chinese":
                            scalarFieldType = ScalarFieldType.STRING;
                            break;
                        case "i64":
                            scalarFieldType = ScalarFieldType.INT64;
                            break;
                        case "f64":
                            scalarFieldType = ScalarFieldType.DOUBLE;
                            break;
                        case "bytes":
                            scalarFieldType = ScalarFieldType.BYTES;
                            break;
                        case "datetime":
                            scalarFieldType = ScalarFieldType.DATETIME;
                            break;
                        case "bool":
                            scalarFieldType = ScalarFieldType.BOOL;
                            break;
                        default:
                            throw new IllegalStateException("Unsupported type: " + type);
                    }
                    scalarSchemaItems.add(ScalarSchemaItem.builder().key(next.getKey().toUpperCase())
                        .fieldType(scalarFieldType).build());
                }
                documentIndexParameter = DocumentIndexParameter.builder()
                    .scalarSchema(ScalarSchema.builder().fields(scalarSchemaItems).build())
                    .jsonParameter(json)
                    .build();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("text_fields format error");
            }
            indexDefinition.setIndexParameter(
                IndexParameter.builder()
                    .indexType(IndexType.INDEX_TYPE_DOCUMENT)
                    .documentIndexParameter(documentIndexParameter)
                    .originKeys(indexDef.getOriginKeyList())
                    .originWithKeys(indexDef.getOriginWithKeyList())
                    .build()
            );
        } else {
            VectorIndexParameter vectorIndexParameter;
            int dimension = Optional.mapOrThrow(
                properties.get("dimension"), Integer::parseInt,
                indexDefinition.getName() + " vector index dimension is null."
            );
            MetricType metricType;
            String metricType1 = properties.getOrDefault("metricType", "L2");
            switch (metricType1.toUpperCase()) {
                case "INNER_PRODUCT":
                    metricType = MetricType.METRIC_TYPE_INNER_PRODUCT;
                    break;
                case "COSINE":
                    metricType = MetricType.METRIC_TYPE_COSINE;
                    break;
                case "L2":
                    metricType = MetricType.METRIC_TYPE_L2;
                    break;
                default:
                    throw new IllegalStateException("Unsupported metric type: " + metricType1);
            }
            switch (properties.getOrDefault("type", "HNSW").toUpperCase()) {
                case "DISKANN":
                    int maxDegree = Integer.parseInt(properties.getOrDefault("max_degree", "64"));
                    int searchListSize = Integer.parseInt(properties.getOrDefault("search_list_size", "100"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_DISKANN)
                        .vectorIndexParameter(
                            DiskannParameter.builder()
                                .dimension(dimension)
                                .metricType(metricType)
                                .maxDegree(maxDegree)
                                .searchListSize(searchListSize)
                                .valueType(ValueType.FLOAT)
                                .codebookPrefix("")
                                .build()
                        )
                        .build();
                    break;
                case "FLAT":
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_FLAT)
                        .vectorIndexParameter(
                            FlatParameter.builder().dimension(dimension).metricType(metricType).build()
                        ).build();
                    break;
                case "IVFPQ": {
                    int ncentroids = Integer.parseInt(properties.getOrDefault("ncentroids", "0"));
                    int nsubvector = Integer.parseInt(Parameters.nonNull(properties.get("nsubvector"), "nsubvector"));
                    int bucketInitSize = Integer.parseInt(properties.getOrDefault("bucketInitSize", "0"));
                    int bucketMaxSize = Integer.parseInt(properties.getOrDefault("bucketMaxSize", "0"));
                    int nbitsPerIdx = Integer.parseInt(properties.getOrDefault("nbitsPerIdx", "0"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_IVF_PQ)
                        .vectorIndexParameter(IvfPqParameter.builder()
                            .dimension(dimension)
                            .metricType(metricType)
                            .ncentroids(ncentroids)
                            .nsubvector(nsubvector)
                            .bucketInitSize(bucketInitSize)
                            .bucketMaxSize(bucketMaxSize)
                            .nbitsPerIdx(nbitsPerIdx)
                            .build()
                        ).build();
                    break;
                }
                case "IVFFLAT": {
                    int ncentroids = Integer.valueOf(properties.getOrDefault("ncentroids", "0"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_IVF_FLAT)
                        .vectorIndexParameter(
                            IvfFlatParameter.builder()
                                .dimension(dimension)
                                .metricType(metricType)
                                .ncentroids(ncentroids)
                                .build()
                        ).build();
                    break;
                }
                case "HNSW": {
                    int efConstruction = Integer.valueOf(properties.getOrDefault("efConstruction", "40"));
                    int nlinks = Integer.valueOf(properties.getOrDefault("nlinks", "32"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_HNSW)
                        .vectorIndexParameter(HnswParameter.builder()
                            .dimension(dimension)
                            .metricType(metricType)
                            .efConstruction(efConstruction)
                            .maxElements(Integer.MAX_VALUE)
                            .nlinks(nlinks)
                            .build()
                        ).build();
                    break;
                }
                default:
                    throw new IllegalStateException("Unsupported type: " + properties.get("type"));
            }

            indexDefinition.setIndexParameter(
                IndexParameter.builder()
                    .indexType(IndexType.INDEX_TYPE_VECTOR)
                    .vectorIndexParameter(vectorIndexParameter)
                    .originKeys(indexDef.getOriginKeyList())
                    .originWithKeys(indexDef.getOriginWithKeyList())
                    .build()
            );
        }
    }

    default boolean checkType(String tantivyType, String sqlType) {
        List<String> strTypes = Arrays.asList("DEFAULT", "RAW", "SIMPLE", "STEM", "WHITESPACE", "NGRAM", "CHINESE");
        switch (sqlType) {
            case "STRING":
            case "VARCHAR":
                return strTypes.contains(tantivyType);
            case "BIGINT":
            case "LONG":
                return tantivyType.equals("I64");
            case "DOUBLE":
                return tantivyType.equals("F64");
            case "BYTES":
                return tantivyType.equals("BYTES");
            case "TIMESTAMP":
                return tantivyType.equals("DATETIME");
            case "BOOLEAN":
                return tantivyType.equals("BOOL");
            default:
                throw new IllegalStateException("Unexpected value: " + tantivyType);
        }
    }

    default ScalarValue scalarValueTo(io.dingodb.store.api.transaction.data.ScalarValue scalarValue) {
        return ScalarValue.builder()
            .fieldType(fieldTypeTo(scalarValue.getFieldType()))
            .fields(scalarValue.getFields().stream()
                .map(f -> scalarFieldTo(f, scalarValue.getFieldType())).collect(Collectors.toList()))
            .build();
    }

    default ScalarField scalarFieldTo(
        io.dingodb.store.api.transaction.data.ScalarField field,
        io.dingodb.store.api.transaction.data.ScalarValue.ScalarFieldType type
    ) {
        switch (type) {
            case BOOL:
                return ScalarField.builder().data(ScalarField.DataNest.BoolData.of((Boolean) field.getData())).build();
            case INTEGER:
                return ScalarField.builder().data(ScalarField.DataNest.IntData.of((Integer) field.getData())).build();
            case LONG:
                return ScalarField.builder().data(ScalarField.DataNest.LongData.of((Long) field.getData())).build();
            case FLOAT:
                return ScalarField.builder().data(ScalarField.DataNest.FloatData.of((Float) field.getData())).build();
            case DOUBLE:
                return ScalarField.builder().data(ScalarField.DataNest.DoubleData.of((Double) field.getData())).build();
            case STRING:
                return ScalarField.builder().data(ScalarField.DataNest.StringData.of((String) field.getData())).build();
            case BYTES:
                return ScalarField.builder().data(ScalarField.DataNest.BytesData.of((byte[]) field.getData())).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    default ScalarFieldType fieldTypeTo(io.dingodb.store.api.transaction.data.ScalarValue.ScalarFieldType fieldType) {
        switch (fieldType) {
            case NONE:
                return ScalarFieldType.NONE;
            case BOOL:
                return ScalarFieldType.BOOL;
            case INTEGER:
                return ScalarFieldType.INT32;
            case LONG:
                return ScalarFieldType.INT64;
            case FLOAT:
                return ScalarFieldType.FLOAT32;
            case DOUBLE:
                return ScalarFieldType.DOUBLE;
            case STRING:
                return ScalarFieldType.STRING;
            case BYTES:
                return ScalarFieldType.BYTES;
            default:
                return ScalarFieldType.UNRECOGNIZED;
        }
    }

}
