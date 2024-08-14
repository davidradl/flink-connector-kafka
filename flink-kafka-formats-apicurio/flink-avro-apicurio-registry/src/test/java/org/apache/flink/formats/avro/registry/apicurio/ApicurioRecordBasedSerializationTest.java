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

import org.apache.flink.formats.avro.AvroFormatOptions;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedSerialization;

import com.google.common.collect.ImmutableList;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.utils.IoUtil;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ApicurioRecordBasedSerialization.
 */
class ApicurioRecordBasedSerializationTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    public void testCustomSerialization(TestSpec testSpec) {
        ApicurioRecordBasedSerializationFactoryImpl apicurioRecordBasedSerializationFactory = new ApicurioRecordBasedSerializationFactoryImpl();
        RecordBasedSerialization recordBasedSerialization =
                apicurioRecordBasedSerializationFactory.create();
        // check empty byte array cannot be processed
        assertThat(recordBasedSerialization.canProcess(new byte[0])).isFalse();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ApicurioSchemaRegistryCoder.augmentOutputStream(out, testSpec.useHeaders,
                    testSpec.encoding, testSpec.globalId, testSpec.contentId);

            // out should not have been augmented.
            final byte[] payload = "TEST PAYLOAD".getBytes();
            out.write(payload, 0, payload.length);
            byte[] customSerialisation = out.toByteArray();
            assertThat(recordBasedSerialization.canProcess(customSerialisation)).isTrue();

            ByteArrayOutputStream expectedPayloadStream = new ByteArrayOutputStream(1 + 8 + payload.length);
            DataOutputStream expectedStream = new DataOutputStream(expectedPayloadStream);
            if (!testSpec.useHeaders) {
                expectedStream.writeByte(0); // magic byte
                Long schemaId = testSpec.globalId == null ? testSpec.contentId : testSpec.globalId;
                expectedStream.writeLong(schemaId);
            }
            expectedStream.write(payload, 0, payload.length);

            byte[] expectedPayload = expectedPayloadStream.toByteArray();

            assertThat(recordBasedSerialization.getPayload(customSerialisation)).isEqualTo(expectedPayload);

            Headers headers = testSpec.isKey ? recordBasedSerialization.getKeyHeaders(customSerialisation) :
                    recordBasedSerialization.getValueHeaders(customSerialisation);
            assertThat(headers.toArray().length).isEqualTo(testSpec.expectedHeaders.size());
            Map<String, Object> expectedHeaders = testSpec.expectedHeaders;
            Iterator<Header> iter = headers.iterator();
            while (iter.hasNext()) {
                Header headerToTest = iter.next();
                Object expectedHeaderValue = expectedHeaders.get(headerToTest.key());
                assertThat(expectedHeaderValue).isEqualTo(headerToTest.value());
            }
            assertThat (testSpec.expectSuccess).isTrue();

        } catch (IOException e) {
            assertThat (testSpec.expectSuccess).isFalse();
        }
    }

    private static class TestSpec {
        boolean isKey;

        boolean useHeaders;

        Long globalId;

        Long contentId;
        AvroFormatOptions.AvroEncoding encoding;

        Map<String, Object> expectedHeaders;
        boolean expectSuccess;

        private TestSpec(
                boolean isKey,
                boolean useHeaders,
                Long globalId,
                Long contentId,
                AvroFormatOptions.AvroEncoding encoding,
                Map<String, Object> expectedHeaders,
                boolean expectSuccess) {
            this.isKey = isKey;
            this.useHeaders = useHeaders;
            this.globalId = globalId;
            this.contentId = contentId;
            this.encoding = encoding;
            this.expectedHeaders = expectedHeaders;
            this.expectSuccess = expectSuccess;
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "isKey="
                    + isKey
                    + "useHeaders="
                    + useHeaders
                    + ", globalId="
                    + globalId
                    + ", contentId="
                    + contentId
                    + ", encoding="
                    + encoding
                    + ", expectedHeaders="
                    + expectedHeaders
                    + ", expectedSuccess="
                    + expectSuccess
                    + '}';
        }
    }

    private static class ValidTestSpec extends TestSpec {
        private ValidTestSpec(
                boolean isKey,
                boolean useHeaders,
                long globalId,
                long contentId,
                AvroFormatOptions.AvroEncoding encoding,
                Map<String, Object> expectedHeaders) {
            super(isKey, useHeaders, globalId, contentId, encoding, expectedHeaders, true);
        }
    }

    private static class InvalidTestSpec extends TestSpec {
        private InvalidTestSpec(
                boolean isKey,
                boolean useHeaders,
                Long globalId,
                Long contentId,
                AvroFormatOptions.AvroEncoding encoding,
                Map<String, Object> expectedHeaders) {
            super(isKey, useHeaders, globalId, contentId, encoding, expectedHeaders, false);
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
                // value binary
                new ValidTestSpec(
                        false,
                        false,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.BINARY,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_VALUE_ENCODING
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.BINARY.name())})
                ),
                // key binary
                new ValidTestSpec(
                        true,
                        false,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.BINARY,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_KEY_ENCODING
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.BINARY.name())})
                ),
                // value json
                new ValidTestSpec(
                        false,
                        false,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.JSON,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_VALUE_ENCODING
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.JSON.name())})
                ),
                // key json
                new ValidTestSpec(
                        true,
                        false,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.JSON,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_KEY_ENCODING
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.JSON.name())})
                ),

                // value binary useHeader
                new ValidTestSpec(
                        false,
                        true,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.BINARY,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_VALUE_ENCODING,
                                        SerdeHeaders.HEADER_VALUE_GLOBAL_ID,
                                        SerdeHeaders.HEADER_VALUE_CONTENT_ID
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.BINARY.name()),
                                        ApicurioSchemaRegistryCoder.longToBytes(13L),
                                        ApicurioSchemaRegistryCoder.longToBytes(12L)})
                ),
                // key binary useHeader
                new ValidTestSpec(
                        true,
                        true,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.BINARY,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_KEY_ENCODING,
                                        SerdeHeaders.HEADER_KEY_GLOBAL_ID,
                                        SerdeHeaders.HEADER_KEY_CONTENT_ID
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.BINARY.name()),
                                        ApicurioSchemaRegistryCoder.longToBytes(13L),
                                        ApicurioSchemaRegistryCoder.longToBytes(12L)})
                ),
                // value json useHeader
                new ValidTestSpec(
                        false,
                        true,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.JSON,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_VALUE_ENCODING,
                                        SerdeHeaders.HEADER_VALUE_GLOBAL_ID,
                                        SerdeHeaders.HEADER_VALUE_CONTENT_ID
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.JSON.name()),
                                        ApicurioSchemaRegistryCoder.longToBytes(13L),
                                        ApicurioSchemaRegistryCoder.longToBytes(12L)})
                ),
                // key json useHeader
                new ValidTestSpec(
                        true,
                        true,
                        13L,
                        12L,
                        AvroFormatOptions.AvroEncoding.JSON,
                        ofEntries(
                                new String[] {SerdeHeaders.HEADER_KEY_ENCODING,
                                        SerdeHeaders.HEADER_KEY_GLOBAL_ID,
                                        SerdeHeaders.HEADER_KEY_CONTENT_ID
                                },
                                new Object[] {IoUtil.toBytes(AvroEncoding.JSON.name()),
                                        ApicurioSchemaRegistryCoder.longToBytes(13L),
                                        ApicurioSchemaRegistryCoder.longToBytes(12L)})

                )
                // the code always generates both global and content headers
        );
    }

    @NotNull
    private static ImmutableList<TestSpec> getInvalidTestSpecs() {
        return ImmutableList.of(

                // tests use_headers and there are no headers
                new InvalidTestSpec(
                        false,
                        false, // send no headers
                        null,
                        null,
                        null,
                        null
                )
        );

    }

    // Cannot use Map.of or Map.ofEntries as we are Java 8.
    private static Map<String, Object> ofEntries(String[] keys, Object[] values) {
        HashMap map = new HashMap();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return map;
    }
}
