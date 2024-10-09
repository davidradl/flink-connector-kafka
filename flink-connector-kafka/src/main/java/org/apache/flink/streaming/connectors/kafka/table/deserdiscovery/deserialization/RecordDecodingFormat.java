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

import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.DataType;

/**
 * Record Decoding Format.
 *
 * @param <I>
 */
public interface RecordDecodingFormat<I> extends Format {
    /**
     * Creates runtime record decoder implementation.
     *
     * @param context the context provides several utilities required to instantiate the runtime
     *     decoder implementation of the format
     * @param physicalDataType For more details check the documentation of {@link DecodingFormat}.
     */
    I createRuntimeRecordDecoder(
            DynamicTableSource.Context context, DataType physicalDataType, boolean isKeyFlag);
}
