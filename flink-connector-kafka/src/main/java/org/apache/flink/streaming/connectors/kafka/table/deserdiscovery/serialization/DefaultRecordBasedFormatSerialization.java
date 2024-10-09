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

package org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization;

import org.apache.kafka.clients.producer.ProducerRecord;

/** Indicates this is record based Serialization. */
public class DefaultRecordBasedFormatSerialization implements RecordBasedFormatSerialization {
    private final boolean isKeyFlag;

    DefaultRecordBasedFormatSerialization(boolean isKeyFlag) {
        this.isKeyFlag = isKeyFlag;
    }

    @Override
    public boolean isKeyFlag() {
        return isKeyFlag;
    }

    @Override
    public byte[] getBytesForFormat(ProducerRecord<byte[], byte[]> record) {
        if (isKeyFlag()) {
            return record.key();
        } else {
            return record.value();
        }
    }
}
