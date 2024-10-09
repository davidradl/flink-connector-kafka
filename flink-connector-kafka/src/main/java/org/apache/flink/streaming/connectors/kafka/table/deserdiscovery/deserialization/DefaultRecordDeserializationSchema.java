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

package org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization;

import org.apache.flink.table.connector.ChangelogMode;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Indicates this is record based deserialization. */
public class DefaultRecordDeserializationSchema implements RecordDeserializationSchema {

    private final boolean isKeyFlag;

    DefaultRecordDeserializationSchema(boolean isKeyFlag) {
        this.isKeyFlag = isKeyFlag;
    }

    @Override
    public boolean isKeyFlag() {
        return isKeyFlag;
    }

    @Override
    public byte[] getBytesForFormat(ConsumerRecord<byte[], byte[]> record) {
        if (isKeyFlag()) {
            return record.key();
        } else {
            return record.value();
        }
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }
}
