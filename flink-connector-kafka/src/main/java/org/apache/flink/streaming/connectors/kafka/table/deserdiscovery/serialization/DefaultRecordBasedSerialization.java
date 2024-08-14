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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Iterator;

/**
 * Indicates this is record based Serialization.
 */
public class DefaultRecordBasedSerialization implements RecordBasedSerialization {
 @Override
 public byte[] getPayload(byte[] customSerialisation) {
  return customSerialisation;
 }

 @Override
 public Headers getKeyHeaders(byte[] customSerialisation) {
  return new Headers() {
   @Override
   public Headers add(Header header) throws IllegalStateException {
    return null;
   }

   @Override
   public Headers add(String key, byte[] value) throws IllegalStateException {
    return null;
   }

   @Override
   public Headers remove(String key) throws IllegalStateException {
    return null;
   }

   @Override
   public Header lastHeader(String key) {
    return null;
   }

   @Override
   public Iterable<Header> headers(String key) {
    return null;
   }

   @Override
   public Header[] toArray() {
    return new Header[0];
   }

   @Override
   public Iterator<Header> iterator() {
    return null;
   }
  };
 }

 @Override
 public Headers getValueHeaders(byte[] customSerialisation) {
  return getKeyHeaders(customSerialisation);
 }

 @Override
 public boolean canProcess(byte[] customSerialisation) {
  throw new RuntimeException("Default canProcess should not be driven.");
 }
}
