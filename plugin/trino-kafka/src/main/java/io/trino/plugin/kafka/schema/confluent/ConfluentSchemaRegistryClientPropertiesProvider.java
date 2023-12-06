/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka.schema.confluent;

import com.google.inject.Inject;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import io.trino.plugin.kafka.utils.PropertiesUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TODO: Complete this javadoc.
 */
public class ConfluentSchemaRegistryClientPropertiesProvider
        implements SchemaRegistryClientPropertiesProvider
{
    private final Map<String, Object> confluentSchemaRegistryClientProperties = new HashMap<>();

    private static final String SCHEMA_REGISTRY_PREFIX = "schema.registry";

    @Inject
    public ConfluentSchemaRegistryClientPropertiesProvider(
            final ConfluentSchemaRegistryConfig confluentSchemaRegistryConfig,
            final KafkaSslConfig kafkaSslConfig,
            final KafkaConfig kafkaConfig)
            throws IOException
    {
        if (confluentSchemaRegistryConfig.getInheritKafkaSslConfiguration()) {
            confluentSchemaRegistryClientProperties.putAll(
                    kafkaSslConfig.getKafkaClientProperties());
        }

        Map<String, String> schemaRegistryFileProperties = PropertiesUtils.readProperties(
                        kafkaConfig.getResourceConfigFiles())
                .entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(SCHEMA_REGISTRY_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        confluentSchemaRegistryClientProperties.putAll(schemaRegistryFileProperties);
    }

    @Override
    public final Map<String, Object> getSchemaRegistryClientProperties()
    {
        return confluentSchemaRegistryClientProperties;
    }
}
