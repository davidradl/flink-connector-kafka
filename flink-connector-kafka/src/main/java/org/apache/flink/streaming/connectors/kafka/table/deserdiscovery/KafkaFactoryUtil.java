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

package org.apache.flink.streaming.connectors.kafka.table.deserdiscovery;

import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.deserialization.RecordBasedDeserialization;
import org.apache.flink.streaming.connectors.kafka.table.deserdiscovery.serialization.RecordBasedSerialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * Factory utils used by the Kafka connector. This is similar to the core Flink processing but the
 * interface does not need to extend the Factory class.
 */
public class KafkaFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFactoryUtil.class);

    /**
     * Loads all factories for the given class using the {@link ServiceLoader} and attempts to
     * create an instance of RecordBasedDeserialization. This interface has a canProcess method,
     * which is called in case we have picked up a class that cannot process the Consumer Record.
     *
     * @param factoryInterface factory interface
     * @param factoryInvoker factory invoker
     * @param <R> resource type this needs to be RecordBasedDeserialization
     * @param <F> factory type
     * @param defaultProvider default provider
     * @throws RuntimeException if multiple resources could be instantiated
     * @return created instance
     */
    public static <R, F> R loadAndInvokeFactoryWithConsumerRecord(
            final Class<F> factoryInterface,
            final FactoryInvoker<F, R> factoryInvoker,
            final Supplier<F> defaultProvider,
            ConsumerRecord<byte[], byte[]> record) {
        final List<Exception> errorsDuringInitialization = new ArrayList<>();
        final List<R> instantiatedResources =
                getInstantiatedResources(
                        factoryInterface, factoryInvoker, errorsDuringInitialization);

        if (instantiatedResources.size() > 0) {
            // we need to try each instantiated resource to check whether it can process the message
            for (int i = 0; i < instantiatedResources.size(); i++) {
                try {
                    RecordBasedDeserialization recordBasedDeserialization =
                            (RecordBasedDeserialization) instantiatedResources.get(i);
                    if (recordBasedDeserialization.canProcess(record)) {
                        return (R) recordBasedDeserialization;
                    }
                } catch (IOException e) {
                    // called an implementation that threw an Exception while trying to process the
                    // consumer record.
                    // TODO log?
                }
            }
        }
        // Did not find an implementation that can process the record, so return the default.
        try {
            return factoryInvoker.invoke(defaultProvider.get());
        } catch (Exception e) {
            final RuntimeException exception =
                    new RuntimeException("Could not instantiate any instance.");
            final RuntimeException defaultException =
                    new RuntimeException("Could not instantiate default instance.", e);
            exception.addSuppressed(defaultException);
            errorsDuringInitialization.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    private static <R, F> List<R> getInstantiatedResources(
            Class<F> factoryInterface,
            FactoryInvoker<F, R> factoryInvoker,
            List<Exception> errorsDuringInitialization) {
        ClassLoader classLoader = factoryInterface.getClassLoader();
        final ServiceLoader<F> factories = ServiceLoader.load(factoryInterface, classLoader);
        final List<R> instantiatedResources = new ArrayList<>();

        Iterator<F> iter = factories.iterator();
        while (iter.hasNext()) {
            F factory = iter.next();
            try {
                R resource = factoryInvoker.invoke(factory);
                instantiatedResources.add(resource);
                LOG.info("Instantiated {}.", resource.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.debug(
                        "Factory {} could not instantiate instance.",
                        factory.getClass().getSimpleName(),
                        e);
                errorsDuringInitialization.add(e);
            }
        }
        return instantiatedResources;
    }

    /**
     * Loads all factories for the given class using the {@link ServiceLoader} and attempts to
     * create an instance of RecordBasedDeserialization. This interface has a canProcess method,
     * which is called in case we have picked up a class that cannot process the Consumer Record.
     *
     * @param factoryInterface factory interface
     * @param factoryInvoker factory invoker
     * @param <R> resource type this needs to be RecordBasedDeserialization
     * @param <F> factory type
     * @param defaultProvider default provider
     * @throws RuntimeException if multiple resources could be instantiated
     * @return created instance
     */
    public static <R, F> R loadAndInvokeFactoryWithCustomSerialization(
            final Class<F> factoryInterface,
            final FactoryInvoker<F, R> factoryInvoker,
            final Supplier<F> defaultProvider,
            byte[] customSerialization) {

        final List<Exception> errorsDuringInitialization = new ArrayList<>();
        final List<R> instantiatedResources =
                getInstantiatedResources(
                        factoryInterface, factoryInvoker, errorsDuringInitialization);

        if (instantiatedResources.size() > 0) {
            // we need to try each instantiated resource to check whether it can process the message
            for (int i = 0; i < instantiatedResources.size(); i++) {
                RecordBasedSerialization recordBasedSerialization =
                        (RecordBasedSerialization) instantiatedResources.get(i);

                if (recordBasedSerialization.canProcess(customSerialization)) {
                    // found an instantiation that can process the message
                    return (R) recordBasedSerialization;
                }
            }
        }

        // Did not find an implementation that can process the record, so return the default.
        try {
            return factoryInvoker.invoke(defaultProvider.get());
        } catch (Exception e) {
            final RuntimeException exception =
                    new RuntimeException("Could not instantiate any instance.");
            final RuntimeException defaultException =
                    new RuntimeException("Could not instantiate default instance.", e);
            exception.addSuppressed(defaultException);
            errorsDuringInitialization.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    /** Interface for invoking the factory. */
    public interface FactoryInvoker<F, R> {
        R invoke(F factory) throws Exception;
    }
}
