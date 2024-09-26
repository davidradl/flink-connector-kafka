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
import org.apache.flink.formats.avro.SchemaCoder;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.types.ArtifactType;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;

/** Reads and Writes schema using Avro Schema Registry protocol. */
public class ApicurioSchemaRegistryCoder implements SchemaCoder {

    private final RegistryClient registryClient;

    private final Map<String, Object> configs;

    private static final int MAGIC_BYTE = 0;

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioSchemaRegistryCoder.class);

    /**
     * Creates {@link SchemaCoder} that uses provided {@link RegistryClient} to connect to schema
     * registry.
     *
     * @param registryClient client to connect schema registry
     * @param configs map for registry configs
     */
    public ApicurioSchemaRegistryCoder(RegistryClient registryClient, Map<String, Object> configs) {
        this.registryClient = registryClient;
        this.configs = configs;
    }

    /**
     * Get the Avro schema using the Apicurio Registry. In order to call the registry, we need to
     * get the schema id of the Avro Schema in the registry. If the format is configured to expect
     * headers, then the schema id will be obtained from the headers. Otherwise, the schema id is
     * obtained from the message payload, depending on the format configuration. The inputstream has
     * been constructed in <code>RecordBasedPayload</code> so contains an indicator as whether this
     * is a key or value, the header and the value or key content. value or the key.
     *
     * @param in input stream
     * @return the Avro Schema
     * @throws IOException if there is an error
     */
    @Override
    public Schema readSchema(InputStream in) throws IOException {
        String methodName = "readSchema";
        if (LOG.isDebugEnabled()) {
            LOG.debug(methodName + " entered");
        }
        if (in == null) {
            return null;
        }
        long schemaId = getSchemaId(in);
        // get the schema in canonical form with references de-referenced
        // TODO what about content id?
        InputStream schemaInputStream = registryClient.getContentByGlobalId(schemaId, true, true);
        Schema schema = new Schema.Parser().parse(schemaInputStream);
        if (LOG.isDebugEnabled()) {
            LOG.debug(methodName + " got schema " + schema + " ID " + schemaId);
        }
        return schema;
    }

    /**
     * Get the schema id.
     *
     * @return the id of the schema
     * @throws IOException error occurred
     */
    protected long getSchemaId(InputStream in) throws IOException {
        long schemaId;
        DataInputStream dataInputStream = new DataInputStream(in);

        boolean useHeaders = (boolean) configs.get(USE_HEADERS.key());

        DeSerializationUtils.readVersionAndFormatName(dataInputStream);
        boolean isKey =
                dataInputStream.readByte() == ApicurioRecordBasedDeserialization.KEY ? true : false;
        Map<String, byte[]> headersMap =
                new ApicurioRecordBasedDeserialization().deserializeToHeaders(dataInputStream);

        if (useHeaders) {
            String globalIDHeaderName =
                    isKey ? SerdeHeaders.HEADER_KEY_GLOBAL_ID : SerdeHeaders.HEADER_VALUE_GLOBAL_ID;
            String contentIDHeaderName =
                    isKey
                            ? SerdeHeaders.HEADER_KEY_CONTENT_ID
                            : SerdeHeaders.HEADER_VALUE_CONTENT_ID;
            byte[] globalIDHeader = headersMap.get(globalIDHeaderName);
            byte[] contentIDHeader = headersMap.get(contentIDHeaderName);
            if (globalIDHeader != null) {
                // if we have a globalId use it, as this is the most accurate way to get a unique
                // schema
                schemaId = bytesToLong(globalIDHeader);
            } else if (contentIDHeader != null) {
                schemaId = bytesToLong(contentIDHeader);
            } else {
                throw new IOException(
                        "The format was configured to enable headers."
                                + "Header names received: "
                                + "Headers received: "
                                + headersMap
                                + "\n "
                                + "Ensure that the kafka message was created by an Apicurio client. Check that it is "
                                + "sending the appropriate ID in the header, if the message is sending the ID in the payload,\n"
                                + " review and amend the format configuration options "
                                + USE_HEADERS);
            }
        } else {
            // the id is in the payload
            if (dataInputStream.readByte() != 0) {
                throw new IOException(
                        "Unknown data format. Magic number was not found. "
                                + USE_HEADERS
                                + "= false"
                                + " configs="
                                + configs);
            }
            schemaId = dataInputStream.readLong();
        }
        return schemaId;
    }

    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {

        String methodName = "writeSchema";
        if (LOG.isDebugEnabled()) {
            LOG.debug(methodName + " entered");
        }

        boolean useHeaders = (boolean) configs.get(USE_HEADERS.key());

        String groupId = (String) configs.get(AvroApicurioFormatOptions.GROUP_ID.key());
        Object artifactIdObject =
                configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_ID.key());
        Object artifactName = configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_NAME.key());

        if (artifactIdObject == null) {
            throw new IOException(
                    "artifactId needs to be configured to identify the associated Apicurio Schema.");
        }
        String artifactId = (String) artifactIdObject;

        if ((boolean)
                configs.getOrDefault(AvroApicurioFormatOptions.REGISTER_SCHEMA.key(), false)) {}

        artifactName = (artifactName == null) ? artifactId : artifactName;

        // config option has a default so no need to check for null
        String artifactDescription =
                (String)
                        configs.get(
                                AvroApicurioFormatOptions.REGISTERED_ARTIFACT_DESCRIPTION.key());
        // config option has a default so no need to check for null
        String artifactVersion =
                (String) configs.get(AvroApicurioFormatOptions.REGISTERED_ARTIFACT_VERSION.key());
        AvroFormatOptions.AvroEncoding avroEncoding =
                (AvroFormatOptions.AvroEncoding) configs.get(AvroFormatOptions.AVRO_ENCODING.key());
        // register the schema
        InputStream in = new ByteArrayInputStream(schema.toString().getBytes());
        ArtifactMetaData artifactMetaData;
        if ((boolean)
                configs.getOrDefault(AvroApicurioFormatOptions.REGISTER_SCHEMA.key(), false)) {
            artifactMetaData =
                    registryClient.createArtifact(
                            groupId,
                            artifactId,
                            artifactVersion,
                            ArtifactType.AVRO,
                            IfExists.RETURN_OR_UPDATE,
                            true,
                            (String) artifactName,
                            artifactDescription,
                            in);
        } else {
            artifactMetaData = registryClient.getArtifactMetaData(groupId, artifactId);
        }
        Long registeredGlobalId = artifactMetaData.getGlobalId();
        Long registeredContentId = artifactMetaData.getContentId();
        augmentOutputStream(out, useHeaders, avroEncoding, registeredGlobalId, registeredContentId);
    }

    /**
     * Augment the output stream to add schema information. Take the output stream and prepend
     * information:
     *
     * <ul>
     *   <li>the version and format name
     *   <li>the header count, followed by a header id and header value
     *   <li>if useHeaders are false, then add a magic byte and schema id
     * </ul>
     *
     * @param out output stream to be augmented
     * @param useHeaders whether to use headers (to send schema information)
     * @param avroEncoding avro encoding (binary or json)
     * @param globalId globalID schema ID
     * @param contentId contentID schema ID
     * @throws IOException io exception attempting to augment the stream.
     */
    public static void augmentOutputStream(
            OutputStream out,
            boolean useHeaders,
            AvroFormatOptions.AvroEncoding avroEncoding,
            Long globalId,
            Long contentId)
            throws IOException {
        // always add the encoding header
        byte encoding = ApicurioRecordBasedSerialization.binaryEncoding;
        if (avroEncoding == AvroFormatOptions.AvroEncoding.JSON) {
            encoding = ApicurioRecordBasedSerialization.jsonEncoding;
        }
        // we always have the encoding header.
        int headerCount = 1;

        DataOutputStream dos = DeSerializationUtils.writeVersionAndFormatName(out);
        if (useHeaders) {
            if (globalId != null) {
                headerCount++;
            }
            if (contentId != null) {
                headerCount++;
            }
        }
        // write out the header count
        dos.writeInt(headerCount);
        // and encoding
        dos.writeByte(ApicurioRecordBasedSerialization.encoding);
        dos.writeByte(encoding);
        if (useHeaders) {
            if (globalId != null) {
                dos.writeByte(ApicurioRecordBasedSerialization.global);
                dos.writeLong(globalId);
            }
            if (contentId != null) {
                dos.writeByte(ApicurioRecordBasedSerialization.content);
                dos.writeLong(contentId);
            } else {
                throw new IOException("Did not find Apicurio headers to identify the schema");
            }
        } else {
            if (contentId == null && globalId == null) {
                throw new IOException("Do not have an Apicurio schema identification");
            }
            dos.writeByte(MAGIC_BYTE);
            byte[] schemaIdBytes;
            Long schemaId = globalId == null ? contentId : globalId;
            schemaIdBytes = ByteBuffer.allocate(8).putLong(schemaId).array();
            dos.write(schemaIdBytes);
        }
    }
}
