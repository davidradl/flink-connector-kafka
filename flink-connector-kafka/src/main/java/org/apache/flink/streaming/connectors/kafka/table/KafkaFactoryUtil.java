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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization.RecordDecodingFormat;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization.RecordDecodingFormatFactory;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedFormatSerialization;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedFormatSerializationFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FormatFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.filterPrefixMapKey;

/** Utility for working with {@link Factory}s. */
@PublicEvolving
public final class KafkaFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactoryUtil.class);

    public static KafkaTableFactoryHelper createKafkaTableFactoryHelper(
            DynamicTableFactory factory, DynamicTableFactory.Context context) {
        return new KafkaTableFactoryHelper(factory, context);
    }

    private static Stream<String> fallbackKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .map(FallbackKey::getKey);
    }

    private static Stream<String> allKeys(ConfigOption<?> option) {
        return Stream.concat(Stream.of(option.key()), fallbackKeys(option));
    }

    private static Set<String> allKeysExpanded(ConfigOption<?> option, Set<String> actualKeys) {
        return allKeysExpanded("", option, actualKeys);
    }

    private static Set<String> allKeysExpanded(
            String prefix, ConfigOption<?> option, Set<String> actualKeys) {
        final Set<String> staticKeys =
                allKeys(option).map(k -> prefix + k).collect(Collectors.toSet());
        if (!canBePrefixMap(option)) {
            return staticKeys;
        }
        // include all prefix keys of a map option by considering the actually provided keys
        return Stream.concat(
                        staticKeys.stream(),
                        staticKeys.stream()
                                .flatMap(
                                        k ->
                                                actualKeys.stream()
                                                        .filter(c -> filterPrefixMapKey(k, c))))
                .collect(Collectors.toSet());
    }

    private static Stream<String> deprecatedKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey);
    }

    /**
     * Helper utility for discovering formats and validating all options for a {@link
     * DynamicTableFactory}.
     *
     * @see #createKafkaTableFactoryHelper(DynamicTableFactory, DynamicTableFactory.Context)
     */
    @PublicEvolving
    public static class KafkaTableFactoryHelper
            extends FactoryUtil.FactoryHelper<DynamicTableFactory> {

        private final DynamicTableFactory.Context context;

        private final Configuration enrichingOptions;

        private KafkaTableFactoryHelper(
                DynamicTableFactory tableFactory, DynamicTableFactory.Context context) {
            super(
                    tableFactory,
                    context.getCatalogTable().getOptions(),
                    FactoryUtil.PROPERTY_VERSION,
                    FactoryUtil.CONNECTOR);
            this.context = context;
            this.enrichingOptions = Configuration.fromMap(context.getEnrichmentOptions());
            this.forwardOptions();
        }

        /**
         * Returns all options currently being consumed by the factory. This method returns the
         * options already merged with {@link DynamicTableFactory.Context#getEnrichmentOptions()},
         * using {@link DynamicTableFactory#forwardOptions()} as reference of mergeable options.
         */
        @Override
        public ReadableConfig getOptions() {
            return super.getOptions();
        }

        public RecordBasedFormatSerialization discoverValueRecordSerializationFormat(
                Class<RecordBasedFormatSerializationFactory>
                        kafkaValueRecordSerializationFormatFactoryClass,
                ConfigOption<String> format) {
            return null;
        }

        public RecordBasedFormatSerialization discoverKeyRecordSerializationFormat(
                Class<RecordBasedFormatSerializationFactory>
                        kafkaValueRecordSerializationFormatFactoryClass,
                ConfigOption<String> format) {
            return null;
        }
        // end new

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalEncodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required sink format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public <I, F extends EncodingFormatFactory<I>>
                Optional<EncodingFormat<I>> discoverOptionalEncodingFormat(
                        Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createEncodingFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory));
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating sink format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        // ----------------------------------------------------------------------------------------

        /**
         * Forwards the options declared in {@link DynamicTableFactory#forwardOptions()} and
         * possibly {@link FormatFactory#forwardOptions()} from {@link
         * DynamicTableFactory.Context#getEnrichmentOptions()} to the final options, if present.
         */
        @SuppressWarnings({"unchecked"})
        private void forwardOptions() {
            for (ConfigOption<?> option : factory.forwardOptions()) {
                enrichingOptions
                        .getOptional(option)
                        .ifPresent(o -> allOptions.set((ConfigOption<? super Object>) option, o));
            }
        }

        private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            final String identifier = allOptions.get(formatOption);
            checkFormatIdentifierMatchesWithEnrichingOptions(formatOption, identifier);
            if (identifier == null) {
                return Optional.empty();
            }
            final F factory =
                    FactoryUtil.discoverFactory(
                            context.getClassLoader(), formatFactoryClass, identifier);
            String formatPrefix = formatPrefix(factory, formatOption);

            // log all used options of other factories
            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptions.stream()
                    .flatMap(
                            option ->
                                    allKeysExpanded(formatPrefix, option, allOptions.keySet())
                                            .stream())
                    .forEach(consumedOptionKeys::add);

            consumedOptions.stream()
                    .flatMap(KafkaFactoryUtil::deprecatedKeys)
                    .map(k -> formatPrefix + k)
                    .forEach(deprecatedOptionKeys::add);

            return Optional.of(factory);
        }

        private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();
            return FactoryUtil.getFormatPrefix(formatOption, identifier);
        }

        @SuppressWarnings({"unchecked"})
        private ReadableConfig createFormatOptions(
                String formatPrefix, FormatFactory formatFactory) {
            Set<ConfigOption<?>> forwardableConfigOptions = formatFactory.forwardOptions();
            Configuration formatConf = new DelegatingConfiguration(allOptions, formatPrefix);
            if (forwardableConfigOptions.isEmpty()) {
                return formatConf;
            }

            Configuration formatConfFromEnrichingOptions =
                    new DelegatingConfiguration(enrichingOptions, formatPrefix);

            for (ConfigOption<?> option : forwardableConfigOptions) {
                formatConfFromEnrichingOptions
                        .getOptional(option)
                        .ifPresent(o -> formatConf.set((ConfigOption<? super Object>) option, o));
            }

            return formatConf;
        }

        /**
         * This function assumes that the format config is used only and only if the original
         * configuration contains the format config option. It will fail if there is a mismatch of
         * the identifier between the format in the plan table map and the one in enriching table
         * map.
         */
        private void checkFormatIdentifierMatchesWithEnrichingOptions(
                ConfigOption<String> formatOption, String identifierFromPlan) {
            Optional<String> identifierFromEnrichingOptions =
                    enrichingOptions.getOptional(formatOption);

            if (!identifierFromEnrichingOptions.isPresent()) {
                return;
            }

            if (identifierFromPlan == null) {
                throw new ValidationException(
                        String.format(
                                "The persisted plan has no format option '%s' specified, while the catalog table has it with value '%s'. "
                                        + "This is invalid, as either only the persisted plan table defines the format, "
                                        + "or both the persisted plan table and the catalog table defines the same format.",
                                formatOption, identifierFromEnrichingOptions.get()));
            }

            if (!Objects.equals(identifierFromPlan, identifierFromEnrichingOptions.get())) {
                throw new ValidationException(
                        String.format(
                                "Both persisted plan table and catalog table define the format option '%s', "
                                        + "but they mismatch: '%s' != '%s'.",
                                formatOption,
                                identifierFromPlan,
                                identifierFromEnrichingOptions.get()));
            }
        }

        /**
         * Discovers a {@link DecodingFormat} of the given type using the given option as factory
         * identifier.
         */
        public <I, F extends RecordDecodingFormatFactory<I>>
                RecordDecodingFormat<I> discoverRecordDecodingFormat(
                        Class<F> formatFactoryClass,
                        ConfigOption<String> formatOption,
                        boolean isKeyFlag) {
            return discoverOptionalRecordDecodingFormat(formatFactoryClass, formatOption, isKeyFlag)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required scan format '%s'.",
                                                    formatOption.key())));
        }

        //        public RecordDecodingFormat<Object>
        // discoverRecordDecodingFormat(Class<RecordDecodingFormatFactory>
        // recordDecodingFormatFactoryClass, ConfigOption<String> valueFormat) {
        //                        //TODO
        //        }
        public <I, F extends RecordDecodingFormatFactory<I>>
                Optional<RecordDecodingFormat<I>> discoverOptionalRecordDecodingFormat(
                        Class<F> formatFactoryClass,
                        ConfigOption<String> formatOption,
                        boolean isKeyFlag) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createRecordDeserializationFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory),
                                            isKeyFlag);
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating scan format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        //        private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
        //                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
        //            final String identifier = allOptions.get(formatOption);
        //            checkFormatIdentifierMatchesWithEnrichingOptions(formatOption, identifier);
        //            if (identifier == null) {
        //                return Optional.empty();
        //            }
        //            final F factory =
        //                    FactoryUtil.discoverFactory(context.getClassLoader(),
        // formatFactoryClass, identifier);
        //            String formatPrefix = formatPrefix(factory, formatOption);
        //
        //            // log all used options of other factories
        //            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
        //            consumedOptions.addAll(factory.requiredOptions());
        //            consumedOptions.addAll(factory.optionalOptions());
        //
        //            consumedOptions.stream()
        //                    .flatMap(
        //                            option ->
        //                                    allKeysExpanded(formatPrefix, option,
        // allOptions.keySet())
        //                                            .stream())
        //                    .forEach(consumedOptionKeys::add);
        //
        //            consumedOptions.stream()
        //                    .flatMap(KafkaFactoryUtil::deprecatedKeys)
        //                    .map(k -> formatPrefix + k)
        //                    .forEach(deprecatedOptionKeys::add);
        //
        //            return Optional.of(factory);
        //        }
    }
}
