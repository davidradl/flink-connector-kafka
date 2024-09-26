/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.KafkaFactoryUtil;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.DefaultRecordBasedSerializationFactory;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedSerialization;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedSerializationFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** SerializationSchema used by {@link KafkaDynamicSink} to configure a {@link KafkaSink}. */
class DynamicKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<RowData> {

    private final Set<String> topics;
    private final Pattern topicPattern;
    private final FlinkKafkaPartitioner<RowData> partitioner;
    @Nullable private final SerializationSchema<RowData> keySerialization;
    private final SerializationSchema<RowData> valueSerialization;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final RowData.FieldGetter[] valueFieldGetters;
    private final boolean hasMetadata;
    private final int[] metadataPositions;
    private final boolean upsertMode;
    private final Map<String, Boolean> topicPatternMatches;

    DynamicKafkaRecordSerializationSchema(
            @Nullable List<String> topics,
            @Nullable Pattern topicPattern,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            boolean upsertMode) {
        if (upsertMode) {
            Preconditions.checkArgument(
                    keySerialization != null && keyFieldGetters.length > 0,
                    "Key must be set in upsert mode for serialization schema.");
        }
        Preconditions.checkArgument(
                (topics != null && topicPattern == null && topics.size() > 0)
                        || (topics == null && topicPattern != null),
                "Either Topic or Topic Pattern must be set.");
        if (topics != null) {
            this.topics = new HashSet<>(topics);
        } else {
            this.topics = null;
        }
        this.topicPattern = topicPattern;
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
        this.valueSerialization = checkNotNull(valueSerialization);
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.upsertMode = upsertMode;
        // Cache results of topic pattern matches to avoid re-evaluating the pattern for each record
        this.topicPatternMatches = new HashMap<>();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData consumedRow, KafkaSinkContext context, Long timestamp) {
        // shortcut in case no input projection is required
        if (keySerialization == null && !hasMetadata) {
            final byte[] valueSerialized = valueSerialization.serialize(consumedRow);

            final String targetTopic = getTargetTopic(consumedRow);

            // the valueSerialized might have been customized by the format to supply headers.
            // So get the headers and values so the format can digest its customized serialization
            // default to existing behaviour.
            Iterable<Header> headers = getHeadersForValue(valueSerialized);
            final byte[] valueSerializedAfterPostProcessing = getSerialization(valueSerialized);

            return new ProducerRecord<>(
                    targetTopic,
                    extractPartition(
                            consumedRow,
                            targetTopic,
                            null,
                            valueSerializedAfterPostProcessing,
                            context.getPartitionsForTopic(targetTopic)),
                    (byte[]) null,
                    valueSerializedAfterPostProcessing,
                    headers);
        }
        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            keySerialized = keySerialization.serialize(keyRow);
        }

        final byte[] valueSerialized;
        final RowKind kind = consumedRow.getRowKind();
        if (upsertMode) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // transform the message as the tombstone message
                valueSerialized = null;
            } else {
                // make the message to be INSERT to be compliant with the INSERT-ONLY format
                final RowData valueRow =
                        DynamicKafkaRecordSerializationSchema.createProjectedRow(
                                consumedRow, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = valueSerialization.serialize(valueRow);
            }
        } else {
            final RowData valueRow =
                    DynamicKafkaRecordSerializationSchema.createProjectedRow(
                            consumedRow, kind, valueFieldGetters);
            valueSerialized = valueSerialization.serialize(valueRow);
        }
        final String targetTopic = getTargetTopic(consumedRow);

        Iterable<Header> valueHeaders = getHeadersForValue(valueSerialized);
        Iterable<Header> keyHeaders = getHeadersForKey(keySerialized);
        Iterable<Header> metadataHeaders =
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS);
        final byte[] valueSerializedAfterPostProcessing = getSerialization(valueSerialized);
        final byte[] keySerializedAfterPostProcessing = getSerialization(keySerialized);
        List<Header> combinedHeaders = new ArrayList<>();

        if (metadataHeaders != null) {
            Iterator<Header> headersIter = metadataHeaders.iterator();
            if (headersIter != null) {
                while (headersIter.hasNext()) {
                    combinedHeaders.add(headersIter.next());
                }
            }
        }

        if (valueHeaders != null) {
            Iterator<Header> headersIter = valueHeaders.iterator();
            if (headersIter != null) {
                while (headersIter.hasNext()) {
                    combinedHeaders.add(headersIter.next());
                }
            }
        }
        if (keyHeaders != null) {
            Iterator<Header> headersIter = keyHeaders.iterator();
            if (headersIter != null) {
                while (headersIter.hasNext()) {
                    combinedHeaders.add(headersIter.next());
                }
            }
        }

        return new ProducerRecord<>(
                targetTopic,
                extractPartition(
                        consumedRow,
                        targetTopic,
                        keySerializedAfterPostProcessing,
                        valueSerializedAfterPostProcessing,
                        context.getPartitionsForTopic(targetTopic)),
                readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
                keySerializedAfterPostProcessing,
                valueSerializedAfterPostProcessing,
                combinedHeaders);
    }

    private byte[] getSerialization(byte[] customSerialization) {
        RecordBasedSerialization recordBasedSerialization =
                getRecordBasedSerializationFactory(customSerialization);
        return recordBasedSerialization.getPayload(customSerialization);
    }

    private Iterable<Header> getHeadersForKey(byte[] customSerialization) {
        RecordBasedSerialization recordBasedSerialization =
                getRecordBasedSerializationFactory(customSerialization);
        return recordBasedSerialization.getKeyHeaders(customSerialization);
    }

    private Iterable<Header> getHeadersForValue(byte[] customSerialization) {
        RecordBasedSerialization recordBasedSerialization =
                getRecordBasedSerializationFactory(customSerialization);
        return recordBasedSerialization.getValueHeaders(customSerialization);
    }

    private RecordBasedSerialization getRecordBasedSerializationFactory(
            byte[] customSerialization) {
        return KafkaFactoryUtil.loadAndInvokeFactoryWithCustomSerialization(
                RecordBasedSerializationFactory.class,
                RecordBasedSerializationFactory::create,
                DefaultRecordBasedSerializationFactory::new,
                customSerialization);
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    private String getTargetTopic(RowData element) {
        if (topics != null && topics.size() == 1) {
            // If topics is a singleton list, we only return the provided topic.
            return topics.stream().findFirst().get();
        }
        final String targetTopic = readMetadata(element, KafkaDynamicSink.WritableMetadata.TOPIC);
        if (targetTopic == null) {
            throw new IllegalArgumentException(
                    "The topic of the sink record is not valid. Expected a single topic but no topic is set.");
        } else if (topics != null && !topics.contains(targetTopic)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The topic of the sink record is not valid. Expected topic to be in: %s but was: %s",
                            topics, targetTopic));
        } else if (topicPattern != null && !cachedTopicPatternMatch(targetTopic)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The topic of the sink record is not valid. Expected topic to match: %s but was: %s",
                            topicPattern, targetTopic));
        }
        return targetTopic;
    }

    private boolean cachedTopicPatternMatch(String topic) {
        return topicPatternMatches.computeIfAbsent(topic, t -> topicPattern.matcher(t).matches());
    }

    private Integer extractPartition(
            RowData consumedRow,
            String targetTopic,
            @Nullable byte[] keySerialized,
            byte[] valueSerialized,
            int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, targetTopic, partitions);
        }
        return null;
    }

    static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, KafkaDynamicSink.WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }
}
