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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Indicates this is record based deserialization.
 */
public class DefaultRecordBasedDeserialization implements RecordBasedDeserialization {
 @Override
 public byte[] getSerializedKeyFromConsumerRecord(ConsumerRecord<byte[], byte[]> record) throws IOException {
     return record.key();
 }

 @Override
 public byte[] getSerializedValueFromConsumerRecord(ConsumerRecord<byte[], byte[]> record) throws IOException{
  return record.value();
 }

 @Override
 public boolean canProcess(ConsumerRecord<byte[], byte[]> record) {
  return false;   // this should not be driven as this class is not a service loader.
 }
}
