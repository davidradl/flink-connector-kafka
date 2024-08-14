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

import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization.RecordBasedDeserialization;

import io.apicurio.registry.serde.SerdeHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.formats.avro.registry.apicurio.ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH;

/** Record based deserialization. */
public class ApicurioRecordBasedDeserialization implements RecordBasedDeserialization {

    public static final byte KEY = (byte) 0;

    public static final byte VALUE = (byte) 1;

    @Override
    public byte[] getSerializedKeyFromConsumerRecord(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return serializeConsumerRecord(record, KEY);
    }

    @Override
    public byte[] getSerializedValueFromConsumerRecord(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return serializeConsumerRecord(record, VALUE);
    }

    private byte[] serializeConsumerRecord(ConsumerRecord<byte[], byte[]> record, byte keyValueIndicator) {
        // the size of the byte array needs to be calculated up from so we can allocate it with the
        // right size.
        // 1 byte for the version
        // 4 byte for the length of the format name
        // length of format name
        // 1 byte to indicate whether this is a key or value
        // 4 bytes to hold the header count integer.

        int sizeOfByteArray = 1 + 4 + FORMAT_NAME_BYTE_ARRAY_LENGTH + 1 + 4;
        Map<String, byte[]> headersMap = new HashMap<>();

        for (Header header: record.headers()) {
            String headerKey = header.key();
            // add the header name size as an int
            sizeOfByteArray = sizeOfByteArray + 4;
            // add the size of the header name
            sizeOfByteArray = sizeOfByteArray + headerKey.getBytes().length;
            // add the header value size
            sizeOfByteArray = sizeOfByteArray + 4;
            // add the size of the header value
            sizeOfByteArray = sizeOfByteArray + header.value().length;
            headersMap.put(headerKey, header.value());
        }
        byte[] data = keyValueIndicator == KEY ? record.key() : record.value();
        sizeOfByteArray = sizeOfByteArray + data.length;
        // Creating a ByteArrayOutputStream with specified size
        ByteArrayOutputStream out = new ByteArrayOutputStream(sizeOfByteArray);
        try {
            DataOutputStream  dos = DeSerializationUtils.writeVersionAndFormatName(out);

            int headersCount = headersMap.size();

            dos.writeByte(keyValueIndicator);
            dos.writeInt(headersCount);
            for (String headerName: headersMap.keySet()) {
                dos.writeInt(headerName.getBytes().length);
                dos.write(headerName.getBytes(), 0, headerName.getBytes().length);
                byte[] headerValue = headersMap.get(headerName);
                dos.writeInt(headerValue.length);
                dos.write(headerValue, 0, headerValue.length);
            }

            dos.write(data, 0, data.length);

            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get Kafka header information from the input stream.
     * Expects an input stream of
     * byte : headers count
     * then for each header:
     * int : header name length
     * byte[] : header name
     * int : header value length
     * value[] : header value
     *
     * @param dataInputStream
     * @return Map of headers (could be empty if the header count in 0)
     * @throws IOException
     */
    Map<String, byte[]> deserializeToHeaders(DataInputStream dataInputStream) throws IOException {
        Map<String, byte[]> headersMap = new HashMap<>();
        int headersCount = dataInputStream.readInt();
        // throw new RuntimeException("deserializeToHeaders headersCount = " + headersCount + "dataInputStream=" + dataInputStream);
        for (int i = 0; i < headersCount; i++) {
            int headerNameLength = dataInputStream.readInt();
            byte[] headerNameByteArray = new byte[headerNameLength];
            dataInputStream.read(headerNameByteArray);
            int headerValueLength = dataInputStream.readInt();
            byte[] headerValueByteArray = new byte[headerValueLength];
            dataInputStream.read(headerValueByteArray);
            headersMap.put(new String(headerNameByteArray), headerValueByteArray);
        }
        return headersMap;
    }

    @Override
    public boolean canProcess(ConsumerRecord<byte[], byte[]> record) {
        Iterator<Header> iter =  record.headers().iterator();
        while (iter.hasNext()) {
            Header header = iter.next();
            // if we have an encoding apicurio header then we have found an Apicurio record.
            if (header.key().equals(SerdeHeaders.HEADER_KEY_ENCODING) ||
                    header.key().equals(SerdeHeaders.HEADER_VALUE_ENCODING)) {
                return true;
            }
        }
        return false;
    }
}
