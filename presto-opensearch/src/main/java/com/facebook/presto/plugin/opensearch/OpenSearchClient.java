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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Client for communicating with OpenSearch cluster.
 * Uses OpenSearch REST API for all operations.
 */
public class OpenSearchClient
{
    private static final Logger log = Logger.get(OpenSearchClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true);
    private static final String METADATA_INDEX = ".presto_metadata";

    private final OpenSearchConfig config;
    private final RestClient restClient;

    @Inject
    public OpenSearchClient(OpenSearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.restClient = createRestClient();
        log.info("OpenSearch client initialized for %s:%d", config.getHost(), config.getPort());
    }

    private RestClient createRestClient()
    {
        HttpHost host = new HttpHost(
                config.getHost(),
                config.getPort(),
                config.isSslEnabled() ? "https" : "http");

        RestClientBuilder builder = RestClient.builder(host)
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(config.getConnectTimeout())
                                .setSocketTimeout(config.getSocketTimeout()));

        // Add authentication if configured
        if (config.getUsername() != null && config.getPassword() != null) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return builder.build();
    }

    public List<String> listIndices()
    {
        try {
            Request request = new Request("GET", "/_cat/indices?format=json&h=index");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                List<String> indices = new ArrayList<>();

                if (root.isArray()) {
                    for (JsonNode node : root) {
                        String indexName = node.get("index").asText();
                        // Filter out system indices (starting with .)
                        if (!indexName.startsWith(".")) {
                            indices.add(indexName);
                        }
                    }
                }

                log.debug("Found %d indices", indices.size());
                return indices;
            }
        }
        catch (IOException e) {
            log.error(e, "Failed to list indices");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to list OpenSearch indices", e);
        }
    }

    public Map<String, Object> getIndexMapping(String indexName)
    {
        try {
            Request request = new Request("GET", "/" + indexName + "/_mapping");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode indexNode = root.get(indexName);

                if (indexNode == null) {
                    return Collections.emptyMap();
                }

                JsonNode mappingsNode = indexNode.get("mappings");
                if (mappingsNode == null) {
                    return Collections.emptyMap();
                }

                JsonNode propertiesNode = mappingsNode.get("properties");
                if (propertiesNode == null) {
                    return Collections.emptyMap();
                }

                // Use LinkedHashMap to preserve field order from OpenSearch
                Map<String, Object> properties = new java.util.LinkedHashMap<>();
                Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    // Convert to LinkedHashMap to preserve nested field order
                    properties.put(field.getKey(), OBJECT_MAPPER.convertValue(field.getValue(), java.util.LinkedHashMap.class));
                }

                log.debug("Retrieved mapping for index %s with %d fields", indexName, properties.size());
                return properties;
            }
        }
        catch (IOException e) {
            log.error(e, "Failed to get mapping for index: %s", indexName);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get OpenSearch index mapping", e);
        }
    }

    public List<Map<String, Object>> executeQuery(String indexName, String query, List<String> fields, int from, int size)
    {
        try {
            // Build search request
            Map<String, Object> searchRequest = new HashMap<>();
            searchRequest.put("from", from);
            searchRequest.put("size", size);

            // Add query
            Map<String, Object> queryMap = OBJECT_MAPPER.readValue(query, Map.class);
            searchRequest.put("query", queryMap);

            // Add source filtering if fields specified
            if (fields != null && !fields.isEmpty()) {
                searchRequest.put("_source", fields);
            }

            String requestBody = OBJECT_MAPPER.writeValueAsString(searchRequest);

            Request request = new Request("POST", "/" + indexName + "/_search");
            request.setJsonEntity(requestBody);

            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode hitsNode = root.get("hits").get("hits");

                List<Map<String, Object>> results = new ArrayList<>();
                if (hitsNode.isArray()) {
                    for (JsonNode hit : hitsNode) {
                        Map<String, Object> document = new HashMap<>();

                        // Add _id
                        document.put("_id", hit.get("_id").asText());

                        // Add _source fields
                        JsonNode sourceNode = hit.get("_source");
                        if (sourceNode != null) {
                            Map<String, Object> source = OBJECT_MAPPER.convertValue(sourceNode, Map.class);
                            document.putAll(source);
                        }

                        results.add(document);
                    }
                }

                log.debug("Query returned %d documents from index %s", results.size(), indexName);
                return results;
            }
        }
        catch (IOException e) {
            log.error(e, "Failed to execute query on index: %s", indexName);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute OpenSearch query", e);
        }
    }

    public List<Map<String, Object>> executeVectorSearch(String indexName, String vectorField, float[] queryVector, int k)
    {
        try {
            // Build k-NN search request
            Map<String, Object> searchRequest = new HashMap<>();
            searchRequest.put("size", k);

            // Build k-NN query
            Map<String, Object> knnQuery = new HashMap<>();
            Map<String, Object> knnField = new HashMap<>();
            knnField.put("vector", queryVector);
            knnField.put("k", k);
            knnQuery.put(vectorField, knnField);

            Map<String, Object> query = new HashMap<>();
            query.put("knn", knnQuery);
            searchRequest.put("query", query);

            String requestBody = OBJECT_MAPPER.writeValueAsString(searchRequest);

            Request request = new Request("POST", "/" + indexName + "/_search");
            request.setJsonEntity(requestBody);

            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode hitsNode = root.get("hits").get("hits");

                List<Map<String, Object>> results = new ArrayList<>();
                if (hitsNode.isArray()) {
                    for (JsonNode hit : hitsNode) {
                        Map<String, Object> document = new HashMap<>();

                        // Add _id and _score
                        document.put("_id", hit.get("_id").asText());
                        document.put("_score", hit.get("_score").asDouble());

                        // Add _source fields
                        JsonNode sourceNode = hit.get("_source");
                        if (sourceNode != null) {
                            Map<String, Object> source = OBJECT_MAPPER.convertValue(sourceNode, Map.class);
                            document.putAll(source);
                        }

                        results.add(document);
                    }
                }

                log.debug("Vector search returned %d documents from index %s", results.size(), indexName);
                return results;
            }
        }
        catch (IOException e) {
            log.error(e, "Failed to execute vector search on index: %s", indexName);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute OpenSearch vector search", e);
        }
    }

    public List<ShardInfo> getShardInfo(String indexName)
    {
        try {
            Request request = new Request("GET", "/_cat/shards/" + indexName + "?format=json&h=shard,node");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                List<ShardInfo> shards = new ArrayList<>();

                if (root.isArray()) {
                    for (JsonNode node : root) {
                        int shardId = node.get("shard").asInt();
                        String nodeAddress = node.get("node").asText();
                        shards.add(new ShardInfo(shardId, nodeAddress));
                    }
                }

                log.debug("Found %d shards for index %s", shards.size(), indexName);
                return shards.isEmpty() ?
                        Collections.singletonList(new ShardInfo(0, config.getHost() + ":" + config.getPort())) :
                        shards;
            }
        }
        catch (IOException e) {
            log.warn(e, "Failed to get shard info for index %s, using default", indexName);
            // Return default single shard on error
            return Collections.singletonList(new ShardInfo(0, config.getHost() + ":" + config.getPort()));
        }
    }
    /**
     * Store column order and type metadata for an index to preserve field order and types.
     * OpenSearch returns fields alphabetically and loses VARCHAR length info, so we store the original order and types.
     */
    public void storeColumnOrderMetadata(String indexName, List<String> columnNames, List<String> columnTypes)
    {
        try {
            // Create metadata index if it doesn't exist
            try {
                Request checkIndex = new Request("HEAD", "/" + METADATA_INDEX);
                restClient.performRequest(checkIndex);
            }
            catch (IOException e) {
                // Index doesn't exist, create it
                Request createIndex = new Request("PUT", "/" + METADATA_INDEX);
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
                restClient.performRequest(createIndex);
                log.info("Created metadata index: %s", METADATA_INDEX);
            }

            // Store column order and types as a document
            Map<String, Object> metadata = new java.util.HashMap<>();
            metadata.put("index_name", indexName);
            metadata.put("column_order", columnNames);
            if (columnTypes != null) {
                metadata.put("column_types", columnTypes);
            }
            metadata.put("timestamp", System.currentTimeMillis());

            String metadataJson = OBJECT_MAPPER.writeValueAsString(metadata);
            Request indexRequest = new Request("PUT", "/" + METADATA_INDEX + "/_doc/" + indexName);
            indexRequest.setJsonEntity(metadataJson);
            restClient.performRequest(indexRequest);

            log.debug("Stored column metadata for index %s: %d columns", indexName, columnNames.size());
        }
        catch (IOException e) {
            log.warn(e, "Failed to store column metadata for index %s", indexName);
            // Non-fatal - we can fall back to alphabetical order
        }
    }

    /**
     * Store column order metadata for an index (backward compatibility).
     */
    public void storeColumnOrderMetadata(String indexName, List<String> columnNames)
    {
        storeColumnOrderMetadata(indexName, columnNames, null);
    }

    /**
     * Retrieve stored column order for an index.
     * Returns empty list if metadata doesn't exist.
     */
    public List<String> getColumnOrderMetadata(String indexName)
    {
        Map<String, List<String>> metadata = getColumnMetadata(indexName);
        return metadata.getOrDefault("column_order", Collections.emptyList());
    }

    /**
     * Retrieve stored column metadata (order and types) for an index.
     * Returns map with "column_order" and "column_types" keys.
     */
    public Map<String, List<String>> getColumnMetadata(String indexName)
    {
        try {
            Request request = new Request("GET", "/" + METADATA_INDEX + "/_doc/" + indexName);
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode source = root.get("_source");
                if (source != null) {
                    Map<String, List<String>> result = new java.util.HashMap<>();

                    // Get column order
                    if (source.has("column_order")) {
                        JsonNode columnOrderNode = source.get("column_order");
                        List<String> columnOrder = new ArrayList<>();
                        if (columnOrderNode.isArray()) {
                            for (JsonNode node : columnOrderNode) {
                                columnOrder.add(node.asText());
                            }
                        }
                        result.put("column_order", columnOrder);
                    }

                    // Get column types
                    if (source.has("column_types")) {
                        JsonNode columnTypesNode = source.get("column_types");
                        List<String> columnTypes = new ArrayList<>();
                        if (columnTypesNode.isArray()) {
                            for (JsonNode node : columnTypesNode) {
                                columnTypes.add(node.asText());
                            }
                        }
                        result.put("column_types", columnTypes);
                    }

                    log.debug("Retrieved column metadata for index %s", indexName);
                    return result;
                }
            }
        }
        catch (IOException e) {
            log.debug("No column metadata found for index %s", indexName);
        }
        return Collections.emptyMap();
    }

    public void close()
    {
        try {
            if (restClient != null) {
                restClient.close();
                log.info("OpenSearch client closed");
            }
        }
        catch (IOException e) {
            log.error(e, "Error closing OpenSearch client");
        }
    }

    /**
     * Information about an OpenSearch shard.
     */
    public static class ShardInfo
    {
        private final int shardId;
        private final String nodeAddress;

        public ShardInfo(int shardId, String nodeAddress)
        {
            this.shardId = shardId;
            this.nodeAddress = nodeAddress;
        }

        public int getShardId()
        {
            return shardId;
        }

        public String getNodeAddress()
        {
            return nodeAddress;
        }
    }
}

// Made with Bob
