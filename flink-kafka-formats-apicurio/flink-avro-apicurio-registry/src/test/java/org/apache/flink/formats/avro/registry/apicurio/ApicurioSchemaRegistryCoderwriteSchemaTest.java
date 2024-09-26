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
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import org.apache.avro.Schema;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for schema coder write schema. */
public class ApicurioSchemaRegistryCoderwriteSchemaTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    public void writeSchema(TestSpec testSpec) {
        try {
            if (testSpec.avroFileName == null) {
                testSpec.avroFileName = "src/test/resources/simpleschema1.avsc";
            }

            // get schema from passed fileName contents
            String schemaStr = readFile(testSpec.avroFileName, StandardCharsets.UTF_8);
            Schema schema = new Schema.Parser().parse(schemaStr);
            // get config
            Map<String, Object> configs = getConfig(testSpec.isKey, testSpec.extraConfigOptions);

            OutputStream out = new ByteArrayOutputStream();
            // create Mock registryClient
            MockRegistryClient registryClient = new MockRegistryClient();
            ArtifactMetaData artifactMetaData = new ArtifactMetaData();
            artifactMetaData.setContentId(testSpec.contentId);
            artifactMetaData.setGlobalId(testSpec.globalId);
            registryClient.setArtifactMetaData(artifactMetaData);

            ApicurioSchemaRegistryCoder apicurioSchemaRegistryCoder =
                    new ApicurioSchemaRegistryCoder(registryClient, configs);

            apicurioSchemaRegistryCoder.writeSchema(schema, out);

            if (testSpec.extraConfigOptions == null) {
                testSpec.extraConfigOptions = new HashMap<>();
            }

            if (testSpec.extraConfigOptions.get(USE_HEADERS.key()) == null) {
                testSpec.extraConfigOptions.put(USE_HEADERS.key(), true);
            }

            boolean useHeaders = (boolean) testSpec.extraConfigOptions.get(USE_HEADERS.key());

            ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInputStream dataInputStream = new DataInputStream(bais);
            assertThat(dataInputStream.readByte())
                    .isEqualTo(ApicurioRegistryAvroFormatFactory.SUPPORTED_VERSION);
            int formatNameLength = dataInputStream.readInt();
            assertThat(formatNameLength)
                    .isEqualTo(ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH);
            byte[] formatNameByteArray =
                    new byte[ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH];
            dataInputStream.read(
                    formatNameByteArray,
                    0,
                    ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH);
            assertThat(formatNameByteArray)
                    .isEqualTo(ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY);
            int headerCount = dataInputStream.readInt();
            // expect at least 1 header
            assertThat(headerCount).isGreaterThan(0);
            Byte encodingHeader = null;
            Long contentIdHeader = null;
            Long globalIdHeader = null;

            for (int i = 0; i < headerCount; i++) {
                byte headerType = dataInputStream.readByte();
                if (headerType == ApicurioRecordBasedSerialization.encoding) {
                    encodingHeader = dataInputStream.readByte();
                } else if (headerType == ApicurioRecordBasedSerialization.content) {
                    contentIdHeader = dataInputStream.readLong();
                } else if (headerType == ApicurioRecordBasedSerialization.global) {
                    globalIdHeader = dataInputStream.readLong();
                }
            }
            // TODO test json encoding
            assertThat(encodingHeader).isEqualTo(ApicurioRecordBasedSerialization.binaryEncoding);
            if (useHeaders) {

                if (testSpec.globalId != null) {
                    assertThat(testSpec.globalId).isEqualTo(globalIdHeader);
                }
                if (testSpec.contentId != null) {
                    assertThat(testSpec.contentId).isEqualTo(contentIdHeader);
                }
            } else {

                // check for the magic byte
                assertThat(dataInputStream.readByte()).isEqualTo((byte) 0);
                Long testProducedId = dataInputStream.readLong();
                if (testSpec.globalId != null) {
                    assertThat(testSpec.globalId).isEqualTo(testProducedId);
                } else if (testSpec.contentId != null) {
                    assertThat(testSpec.contentId).isEqualTo(testProducedId);
                }
            }
            assertThat(testSpec.expectSuccess).isTrue();
        } catch (Exception e) {
            assertThat(testSpec.expectSuccess).isFalse();
        }
    }

    private static class TestSpec {

        boolean isKey;
        Map<String, Object> extraConfigOptions;
        String avroFileName;
        Long contentId;
        Long globalId;
        boolean expectSuccess;

        private TestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String avroFileName,
                Long contentId,
                Long globalId,
                boolean expectSuccess) {
            this.extraConfigOptions = extraConfigOptions;
            this.isKey = isKey;
            this.avroFileName = avroFileName;
            this.contentId = contentId;
            this.globalId = globalId;
            this.expectSuccess = expectSuccess;
        }

        @Override
        public String toString() {
            String extraConfigOptionsStr = "";
            if (MapUtils.isNotEmpty(this.extraConfigOptions)) {
                for (String key : this.extraConfigOptions.keySet()) {
                    extraConfigOptionsStr =
                            extraConfigOptionsStr
                                    + "("
                                    + key
                                    + ","
                                    + this.extraConfigOptions.get(key)
                                    + ")\n";
                }
            }

            return "TestSpec{"
                    + "extraConfigOptions="
                    + extraConfigOptionsStr
                    + ", isKey="
                    + isKey
                    + ", avroFileName="
                    + avroFileName
                    + ", contentId="
                    + contentId
                    + ", globalId="
                    + globalId
                    + ", expectSuccess="
                    + expectSuccess
                    + '}';
        }
    }

    private static class ValidTestSpec extends TestSpec {
        private ValidTestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String avroFileName,
                Long contentId,
                Long globalId) {
            super(isKey, extraConfigOptions, avroFileName, contentId, globalId, true);
        }
    }

    private static class InvalidTestSpec extends TestSpec {
        private InvalidTestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String avroFileName,
                Long contentId,
                Long globalId) {
            super(isKey, extraConfigOptions, avroFileName, contentId, globalId, false);
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
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        12L,
                        13L),
                // value global
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        12L,
                        13L),
                // key content
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        12L,
                        13L),
                // value content
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        12L,
                        13L),
                // Key global Legacy
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {USE_HEADERS.key(), REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {false, "artifactId1"}),
                        null,
                        12L,
                        13L),
                // Value global Legacy
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {USE_HEADERS.key(), REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {false, "artifactId1"}),
                        null,
                        12L,
                        13L),
                // Key content Legacy
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        12L,
                        13L),
                // Value content Legacy
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {USE_HEADERS.key(), REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {false, "artifactId1"}),
                        null,
                        12L,
                        13L));
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
        // no artifact id configured
        return ImmutableList.of(
                // no artifact id supplied in the config
                new InvalidTestSpec(true, null, null, 12L, 13L),
                // no ids supplied
                new InvalidTestSpec(
                        true,
                        ofEntries(
                                new String[] {REGISTERED_ARTIFACT_ID.key()},
                                new Object[] {"artifactId1"}),
                        null,
                        null,
                        null));
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
            Object value = null;
            String configOptionKey = configOption.key();
            if (extraConfigOptions != null) {
                value = extraConfigOptions.get(configOptionKey);
            }
            if (value == null && configOption.hasDefaultValue()) {
                value = configOption.defaultValue();
            }
            registryConfigs.put(configOptionKey, value);
        }
        return registryConfigs;
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
