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

import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedSerialization;

import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.utils.IoUtil;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/** Record based deserialization. */
public class ApicurioRecordBasedSerialization implements RecordBasedSerialization {

    public static byte encoding = (byte) 0;

    public static byte content = (byte) 1;

    public static byte global = (byte) 2;

    public static byte binaryEncoding = (byte) 0;

    public static byte jsonEncoding = (byte) 1;

    @Override
    public Headers getKeyHeaders(byte[] customSerialisation) {
        return getHeaders(customSerialisation, true);
    }

    @Override
    public Headers getValueHeaders(byte[] customSerialisation) {
        return getHeaders(customSerialisation, false);
    }

    @Override
    public byte[] getPayload(byte[] inputSerialisation) {
        // the size of the byte array needs to be calculated up from so we can allocate it with the
        // right size.
        // 1 byte for the version
        // 1 int for the id count
        // then or each id
        // a byte for the id type followed by a long of the id

        byte[] customSerialisation = inputSerialisation.clone();
        InputStream is = new ByteArrayInputStream(customSerialisation);
        DataInputStream dataInputStream = new DataInputStream(is);
        int startArray = 0;
        try {
            DeSerializationUtils.readVersionAndFormatName(dataInputStream);
            startArray = startArray + 1 + 4 + ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH;
            int idCount = dataInputStream.readInt();       // id count
            startArray = startArray + 4;
            // for each id
            for (int i = 0; i < idCount; i++) {
                byte id = dataInputStream.readByte();  // id type
                startArray++;
                if (id == ApicurioRecordBasedSerialization.encoding) {
                    dataInputStream.readByte();
                    startArray++;
                } else {
                    final byte[] longHeaderValue = new byte[8];
                    dataInputStream.read(longHeaderValue, 0, 8);
                    startArray = startArray + 8;
                }
            }
        } catch (IOException e) {
            //TODO log
        }

        return Arrays.copyOfRange(customSerialisation, startArray, customSerialisation.length);

    }
//
//    private static byte[] readFully(InputStream input) throws IOException {
//        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(input))) {
//            return buffer.lines().collect(Collectors.joining("\n")).getBytes();
//        }
//    }

    public Headers getHeaders(byte[] customSerialisation, boolean isKey) {
        // the size of the byte array needs to be calculated up front, so we can allocate it with the
        // right size.
        // 1 byte for the version
        // 1 int for the id count
        // then or each id
        // a byte for the id type followed by a long of the id
        Headers headers = new RecordHeaders();
        InputStream is = new ByteArrayInputStream(customSerialisation);
        DataInputStream dataInputStream = new DataInputStream(is);
        try {
            DeSerializationUtils.readVersionAndFormatName(dataInputStream);
            int idCount = dataInputStream.readInt();       // id count
            // for each id
            for (int i = 0; i < idCount; i++) {
                String headerName = null;
                byte[] value;
                byte idType = dataInputStream.readByte();  // id type
                if (idType == ApicurioRecordBasedSerialization.encoding) {
                    byte encodingFlag = dataInputStream.readByte();

                    if (isKey) {
                        headerName = SerdeHeaders.HEADER_KEY_ENCODING;
                    } else {
                        headerName = SerdeHeaders.HEADER_VALUE_ENCODING;
                    }
                    if (encodingFlag == ApicurioRecordBasedSerialization.binaryEncoding) {
                        // Apicurio uses the IoUtil library in this way.
                        value = IoUtil.toBytes(AvroEncoding.BINARY.name());
                    } else {
                        value = IoUtil.toBytes(AvroEncoding.JSON.name());
                    }
                } else {
                    final byte[] longHeaderValue = new byte[8];
                    dataInputStream.read(longHeaderValue, 0, 8);
                    value = longHeaderValue;

                    if (idType == global) {
                        if (isKey) {
                            headerName = SerdeHeaders.HEADER_KEY_GLOBAL_ID;
                        } else {
                            headerName = SerdeHeaders.HEADER_VALUE_GLOBAL_ID;
                        }
                    } else if (idType == content) {
                        if (isKey) {
                            headerName = SerdeHeaders.HEADER_KEY_CONTENT_ID;
                        } else {
                            headerName = SerdeHeaders.HEADER_VALUE_CONTENT_ID;
                        }
                    }
                }

                final String headerKey = headerName;
                final byte[] headerValue = value;
                //final byte[] headerValueBA =
                Header header = new Header() {
                    @Override
                    public String key() {
                        return headerKey;
                    }

                    @Override
                    public byte[] value() {
                        return headerValue;
                    }
                };
                headers.add(header);

            }
        } catch (IOException e) {
            //TODO log
        }
        return headers;
    }

    @Override
    public boolean canProcess(byte[] customSerialisation) {
        InputStream is = new ByteArrayInputStream(customSerialisation);
        DataInputStream dataInputStream = new DataInputStream(is);
        boolean canProcess = false;
        try {
            DeSerializationUtils.readVersionAndFormatName(dataInputStream);
            canProcess = true;
        } catch (Exception e) {
           // ignore any exception as this indicates we cannot process this message
        }
        return canProcess;
    }
}
