/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.configuration.ConfigOption;

import com.google.common.collect.ImmutableList;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.avro.AvroEncoding;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for schemacoder getschema. */
public class ApicurioSchemaRegistryCodergetSchemaTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    public void getSchemaId(TestSpec testSpec) {
        try {

            if (testSpec.extraConfigOptions == null) {
                testSpec.extraConfigOptions = new HashMap<>();
                testSpec.extraConfigOptions.put(USE_HEADERS.key(), true);
            }

            // get options map
            Map<String, Object> registryConfigs =
                    getConfig(testSpec.isKey, testSpec.extraConfigOptions);
            // this is just to have something in the stream
            String str = readFile("src/test/resources/simple1.avro", StandardCharsets.UTF_8);

            byte[] avroMessage = str.getBytes(StandardCharsets.UTF_8);

            byte[] message;
            if (testSpec.byteArray == null) {
                message = avroMessage;
            } else {
                // the byteArray is the magic byte and schema id that should be at the start of the
                // body
                message = new byte[testSpec.byteArray.length + avroMessage.length];
                ByteBuffer byteBuffer = ByteBuffer.wrap(message);
                byteBuffer.put(testSpec.byteArray);
                byteBuffer.put(avroMessage);
                message = byteBuffer.array();
            }
            Headers headers = convertMapToHeaders(testSpec.headers);
            ApicurioRecordBasedDeserialization recordBasedPayloadDeserialization = (ApicurioRecordBasedDeserialization) new ApicurioRecordBasedDeserializationFactoryImpl().create();

            ConsumerRecord record = new ConsumerRecord("topic",
                    1, 0L,
                    0L,
                    TimestampType.NO_TIMESTAMP_TYPE,
                    message.length,   // key length
                    message.length,   // value length
                    message,
                    message,
                    headers,
                    null
            );

            byte[] deserCustomSerialization;
            if (testSpec.isKey) {
                deserCustomSerialization = recordBasedPayloadDeserialization.getSerializedKeyFromConsumerRecord(record);
            } else {
                deserCustomSerialization = recordBasedPayloadDeserialization.getSerializedValueFromConsumerRecord(record);
            }
            InputStream in = new ByteArrayInputStream(deserCustomSerialization);
            ApicurioSchemaRegistryCoder apicurioSchemaRegistryCoder =
                    new ApicurioSchemaRegistryCoder(null, registryConfigs);
            if (testSpec.extraConfigOptions == null) {
                testSpec.extraConfigOptions = new HashMap<>();
            }

            Long testSchemaId = apicurioSchemaRegistryCoder.getSchemaId(in);
            assertThat(testSchemaId).isEqualTo(testSpec.expectedId);
            assertThat(testSpec.expectSuccess).isTrue();

        } catch (Exception e) {
            assertThat(testSpec.expectSuccess).isFalse();
        }
    }

    private Headers convertMapToHeaders(Map<String, Object> mapHeaders) {
        Headers headers = new RecordHeaders();
        if (mapHeaders != null && !mapHeaders.isEmpty()) {
            mapHeaders.forEach(
                    (k, v) -> {
                        if (k != null) {
                            Header header =
                                    new Header() {
                                        @Override
                                        public String key() {
                                            return k;
                                        }

                                        @Override
                                        public byte[] value() {
                                            return (byte[]) v;
                                        }
                                    };
                            headers.add(header);
                        }
                    });
        }
        return headers;
    }

    private static class TestSpec {

        boolean isKey;
        Map<String, Object> headers;
        byte[] byteArray;
        Map<String, Object> extraConfigOptions;
        long expectedId;
        boolean expectSuccess;

        private TestSpec(
                boolean isKey,
                Map<String, Object> headers,
                byte[] byteArray,
                Map<String, Object> extraConfigOptions,
                long expectedId,
                boolean expectSuccess) {
            this.isKey = isKey;
            this.headers = headers;
            this.byteArray = byteArray;
            this.extraConfigOptions = extraConfigOptions;
            this.expectedId = expectedId;
            this.expectSuccess = expectSuccess;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "isKey="
                    + isKey
                    + ", headers="
                    + headers
                    + ", byteArray="
                    + byteArray
                    + ", extraConfigOptions="
                    + extraConfigOptions
                    + ", expectedId="
                    + expectedId
                    + '}';
        }
    }

    private static class ValidTestSpec extends TestSpec {
        private ValidTestSpec(
                boolean isKey,
                Map<String, Object> headers,
                byte[] byteArray,
                Map<String, Object> extraConfigOptions,
                long expectedId) {
            super(isKey, headers, byteArray, extraConfigOptions, expectedId, true);
        }
    }

    private static class InvalidTestSpec extends TestSpec {
        private InvalidTestSpec(
                boolean isKey,
                Map<String, Object> headers,
                byte[] byteArray,
                Map<String, Object> extraConfigOptions,
                long expectedId) {
            super(isKey, headers, byteArray, extraConfigOptions, expectedId, false);
        }
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder()
                .addAll(getValidTestSpecs())
                .addAll(getInvalidTestSpecs())
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getValidTestSpecs() {
        return ImmutableList.of(
                // Key global
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_KEY_GLOBAL_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)}),
                        null,
                        null,
                        12L),
                // key global and content
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_KEY_CONTENT_ID, SerdeHeaders.HEADER_KEY_GLOBAL_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12), ApicurioSchemaRegistryCoder.longToBytes(13)}),
                        null,
                        null,
                        13L),
                // Value global
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_VALUE_GLOBAL_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)}),
                        null,
                        null,
                        12L),
                // Key content
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_KEY_CONTENT_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)}),
                        null,
                        null,
                        12L),
                // Value content
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_VALUE_CONTENT_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)}),
                        null,
                        null,
                        12L),
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_VALUE_CONTENT_ID, SerdeHeaders.HEADER_VALUE_GLOBAL_ID
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12), ApicurioSchemaRegistryCoder.longToBytes(13)}),
                        null,
                        null,
                        13L),
                // test legacy
                new ValidTestSpec(
                        false,
                        null, // send no headers
                        getLegacyByteArray(12L),
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {false}),
                        12L));
    }

    private static byte[] getLegacyByteArray(long schemaId) {
        byte[] legacyByteArray = new byte[1 + Long.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(legacyByteArray);
        // magic byte
        byteBuffer.put(new byte[] {0});
        // 8 byte schema id
        byteBuffer.put(ApicurioSchemaRegistryCoder.longToBytes(schemaId));
        return byteBuffer.array();
    }

    @NotNull
    private static ImmutableList<TestSpec> getInvalidTestSpecs() {
        return ImmutableList.of(

                // tests use_headers and there are no headers
                new InvalidTestSpec(
                        false,
                        null, // send no headers
                        null,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {true}),
                        12L),
                new InvalidTestSpec(
                        false,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_KEY_ENCODING
                                },
                                new Object[] {AvroEncoding.BINARY.name().getBytes()})
                        , // send only value encoding header
                        null,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {true}),
                        12L),
                new InvalidTestSpec(
                        false,
                        ofEntries(
                                new String[] {
                                        SerdeHeaders.HEADER_VALUE_ENCODING
                                },
                                new Object[] {AvroEncoding.BINARY.name().getBytes()})
                        , // send only encoding header
                        null,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {true}),
                        12L),

                // tests LEGACY with no magic byte
                new InvalidTestSpec(
                        false,
                        null, // send no headers
                        null,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {false}),
                        12L));
    }

    // Cannot use Map.of or Map.ofEntries as we are Java 8.
    private static Map<String, Object> ofEntries(String[] keys, Object[] values) {
        HashMap map = new HashMap();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return map;
    }

    @NotNull
    private static Map<String, Object> getConfig(
            boolean isKey, // TODO do we need to handle value and key specific map keys or not?
            Map<String, Object> extraConfigOptions) {
        Set<ConfigOption<?>> configOptions = ApicurioRegistryAvroFormatFactory.getOptionalOptions();
        Map<String, Object> registryConfigs = new HashMap<>();
        for (ConfigOption configOption : configOptions) {
            Object value = extraConfigOptions.get(configOption.key());
            if (value == null && configOption.hasDefaultValue()) {
                value = configOption.defaultValue();
            }
            registryConfigs.put(configOption.key(), value);
        }
        return registryConfigs;
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
