package org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FormatFactory;

/**
 * Base interface for configuring a {@link DecodingFormat} for {@link ScanTableSource} and {@link
 * LookupTableSource}.
 *
 * <p>Depending on the kind of external system, a connector might support different encodings for
 * reading and writing rows. This interface helps in making such formats pluggable.
 *
 * <p>The created {@link Format} instance is an intermediate representation that can be used to
 * construct runtime implementation in a later step.
 *
 * @see FactoryUtil#createTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
 * @param <I> runtime interface needed by the table source
 */
@PublicEvolving
public interface DecodingRecordFormatFactory<I> extends FormatFactory {

    /**
     * Creates a format from the given context and format options.
     *
     * <p>The format options have been projected to top-level options (e.g. from {@code
     * format.ignore-errors} to {@code ignore-errors}).
     */
    RecordBasedFormatDeserialization<I> createRecordBasedFormatDeserialization(
            DynamicTableFactory.Context context, ReadableConfig formatOptions);
}
