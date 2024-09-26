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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/** Default RecordBased Serialization Factory. */
public class DeSerializationUtils {

    // TODO remove inputSerialisation only there for debug
    public static void readVersionAndFormatName(DataInputStream dataInputStream)
            throws IOException {
        byte version = dataInputStream.readByte();
        if (version != ApicurioRegistryAvroFormatFactory.SUPPORTED_VERSION) {
            // TODO error
        }
        int formatNameLength = dataInputStream.readInt();
        if (formatNameLength != ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH) {
            throw new RuntimeException(
                    "Serialization error format name length incorrect "
                            + "got formatNameLength="
                            + formatNameLength
                            + ", expected "
                            + ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH
                            + ",dataInputStream="
                            + dataInputStream
                            + ",inputSerialisation=");
        }
        byte[] formatNameByteArray =
                new byte[ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH];
        dataInputStream.read(
                formatNameByteArray,
                0,
                ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH);
        if (new String(formatNameByteArray)
                .equals(ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY)) {
            // TODO error
            throw new RuntimeException("Format name error");
        }
    }

    public static DataOutputStream writeVersionAndFormatName(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(ApicurioRegistryAvroFormatFactory.SUPPORTED_VERSION);
        dos.writeInt(ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH);
        dos.write(
                ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY,
                0,
                ApicurioRegistryAvroFormatFactory.FORMAT_NAME_BYTE_ARRAY_LENGTH);
        return dos;
    }
}
