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
import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

/**
 * Comprehensive integration tests for OpenSearch connector.
 * Extends AbstractTestQueries to inherit standard query test suite (500+ tests)
 * and adds OpenSearch-specific tests including nested field queries.
 */
public class TestOpenSearchQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestOpenSearchQueries.class);
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.1";
    private static volatile boolean indexCreated;
    private static final Object INDEX_LOCK = new Object();

    private static GenericContainer<?> opensearchContainer;
    private static RestClient restClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Start OpenSearch container only once (called by framework before @BeforeClass)
        if (opensearchContainer == null) {
            synchronized (INDEX_LOCK) {
                if (opensearchContainer == null) {
                    opensearchContainer = new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                            .withExposedPorts(9200, 9600)
                            .withEnv("discovery.type", "single-node")
                            .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                            .withEnv("DISABLE_SECURITY_PLUGIN", "true")
                            .waitingFor(new HttpWaitStrategy()
                                    .forPort(9200)
                                    .forStatusCode(200)
                                    .withStartupTimeout(Duration.ofMinutes(2)));

                    opensearchContainer.start();

                    log.info("OpenSearch container started at: http://%s:%d",
                            opensearchContainer.getHost(),
                            opensearchContainer.getMappedPort(9200));

                    // Create REST client for index setup
                    restClient = RestClient.builder(
                            new HttpHost(
                                    opensearchContainer.getHost(),
                                    opensearchContainer.getMappedPort(9200),
                                    "http"))
                            .build();

                    // Create test indices with nested fields
                    createTestIndices();
                }
            }
        }

        return OpenSearchQueryRunner.createQueryRunner(opensearchContainer, TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (restClient != null) {
            restClient.close();
        }
        if (opensearchContainer != null) {
            opensearchContainer.stop();
        }
    }

    /**
     * Helper method to create a session for OpenSearch-specific queries.
     * Uses the opensearch_with_id catalog which has _id column visible.
     */
    private Session createOpenSearchSession()
    {
        return testSessionBuilder()
                .setCatalog("opensearch_with_id")
                .setSchema("default")
                .build();
    }

    /**
     * Create test indices with nested field mappings and sample data.
     */
    private void createTestIndices()
            throws IOException
    {
        // Only create index once across all test instances (double-check locking)
        synchronized (INDEX_LOCK) {
            if (indexCreated) {
                log.info("Index already created, skipping creation");
                return;
            }

            // Delete index if it exists to ensure clean state
            try {
                Request deleteRequest = new Request("DELETE", "/analytics_query_index");
                restClient.performRequest(deleteRequest);
                log.info("Deleted existing analytics_query_index");
            }
            catch (Exception e) {
                // Index doesn't exist, which is fine
                log.info("Index analytics_query_index does not exist, creating new");
            }

            // Create analytics_query_index with nested fields
            String analyticsMapping = "{\n" +
                    "  \"mappings\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"user_id\": { \"type\": \"keyword\" },\n" +
                    "      \"query_text\": { \"type\": \"text\" },\n" +
                    "      \"token_usage\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"total_tokens\": { \"type\": \"long\" },\n" +
                    "          \"prompt_tokens\": { \"type\": \"long\" },\n" +
                    "          \"completion_tokens\": { \"type\": \"long\" }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"reliability_scores\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"answer_relevance\": {\n" +
                    "            \"type\": \"object\",\n" +
                    "            \"properties\": {\n" +
                    "              \"score\": { \"type\": \"integer\" },\n" +
                    "              \"confidence\": { \"type\": \"double\" }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"faithfulness\": {\n" +
                    "            \"type\": \"object\",\n" +
                    "            \"properties\": {\n" +
                    "              \"score\": { \"type\": \"integer\" }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"metadata\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"model_name\": { \"type\": \"keyword\" },\n" +
                    "          \"timestamp\": { \"type\": \"date\" }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            Request createIndexRequest = new Request("PUT", "/analytics_query_index");
            createIndexRequest.setJsonEntity(analyticsMapping);
            Response response = restClient.performRequest(createIndexRequest);
            assertEquals(response.getStatusLine().getStatusCode(), 200);

            log.info("Created analytics_query_index");

            // Insert sample documents
            insertSampleDocuments();

            // Mark index as created
            indexCreated = true;
        }
    }

    /**
     * Insert sample documents with nested field data.
     */
    private void insertSampleDocuments()
            throws IOException
    {
        // Document 1
        String doc1 = "{\n" +
                "  \"user_id\": \"user1\",\n" +
                "  \"query_text\": \"What is machine learning?\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 90616,\n" +
                "    \"prompt_tokens\": 45308,\n" +
                "    \"completion_tokens\": 45308\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 80,\n" +
                "      \"confidence\": 0.85\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 75\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-4\",\n" +
                "    \"timestamp\": \"2024-01-01T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc1 = new Request("PUT", "/analytics_query_index/_doc/test-id-1");
        indexDoc1.setJsonEntity(doc1);
        restClient.performRequest(indexDoc1);

        // Document 2
        String doc2 = "{\n" +
                "  \"user_id\": \"user2\",\n" +
                "  \"query_text\": \"Explain neural networks\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 120000,\n" +
                "    \"prompt_tokens\": 60000,\n" +
                "    \"completion_tokens\": 60000\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 90,\n" +
                "      \"confidence\": 0.92\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 85\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-4\",\n" +
                "    \"timestamp\": \"2024-01-02T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc2 = new Request("PUT", "/analytics_query_index/_doc/test-id-2");
        indexDoc2.setJsonEntity(doc2);
        restClient.performRequest(indexDoc2);

        // Document 3
        String doc3 = "{\n" +
                "  \"user_id\": \"user1\",\n" +
                "  \"query_text\": \"Deep learning basics\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 45000,\n" +
                "    \"prompt_tokens\": 22500,\n" +
                "    \"completion_tokens\": 22500\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 70,\n" +
                "      \"confidence\": 0.78\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 65\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-3.5\",\n" +
                "    \"timestamp\": \"2024-01-03T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc3 = new Request("PUT", "/analytics_query_index/_doc/test-id-3");
        indexDoc3.setJsonEntity(doc3);
        restClient.performRequest(indexDoc3);

        // Refresh index to make documents searchable
        Request refreshRequest = new Request("POST", "/analytics_query_index/_refresh");
        restClient.performRequest(refreshRequest);

        log.info("Inserted sample documents into analytics_query_index");

        // Create vector embeddings index for vector search tests
        createVectorEmbeddingsIndex();
    }

    /**
     * Create a separate index with vector embeddings stored as arrays for testing vector search functions.
     */
    private void createVectorEmbeddingsIndex()
            throws IOException
    {
        // Delete index if it exists
        try {
            Request deleteRequest = new Request("DELETE", "/vector_docs");
            restClient.performRequest(deleteRequest);
            log.info("Deleted existing vector_docs index");
        }
        catch (Exception e) {
            log.info("vector_docs index does not exist, creating new");
        }

        // Create index with explicit float mapping for embedding field
        // OpenSearch will store array values in this field and include them in _source
        // Note: We cannot use knn_vector type because it doesn't support retrieval via _source or fields API
        String vectorMapping = "{\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"doc_id\": { \"type\": \"keyword\" },\n" +
                "      \"text\": { \"type\": \"text\" },\n" +
                "      \"embedding\": { \"type\": \"float\" },\n" +
                "      \"vec_x\": { \"type\": \"float\" },\n" +
                "      \"vec_y\": { \"type\": \"float\" },\n" +
                "      \"vec_z\": { \"type\": \"float\" }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Request createVectorIndex = new Request("PUT", "/vector_docs");
        createVectorIndex.setJsonEntity(vectorMapping);
        Response response = restClient.performRequest(createVectorIndex);
        assertEquals(response.getStatusLine().getStatusCode(), 200);

        log.info("Created vector_docs index");

        // Insert documents with both array embeddings and individual components
        // Document 1: [1.0, 0.0, 0.0] - unit vector along x-axis
        String vecDoc1 = "{\n" +
                "  \"doc_id\": \"vec-1\",\n" +
                "  \"text\": \"machine learning\",\n" +
                "  \"embedding\": [1.0, 0.0, 0.0],\n" +
                "  \"vec_x\": 1.0,\n" +
                "  \"vec_y\": 0.0,\n" +
                "  \"vec_z\": 0.0\n" +
                "}";

        Request indexVecDoc1 = new Request("PUT", "/vector_docs/_doc/vec-1");
        indexVecDoc1.setJsonEntity(vecDoc1);
        restClient.performRequest(indexVecDoc1);

        // Document 2: [0.0, 1.0, 0.0] - unit vector along y-axis (orthogonal to doc1)
        String vecDoc2 = "{\n" +
                "  \"doc_id\": \"vec-2\",\n" +
                "  \"text\": \"deep learning\",\n" +
                "  \"embedding\": [0.0, 1.0, 0.0],\n" +
                "  \"vec_x\": 0.0,\n" +
                "  \"vec_y\": 1.0,\n" +
                "  \"vec_z\": 0.0\n" +
                "}";

        Request indexVecDoc2 = new Request("PUT", "/vector_docs/_doc/vec-2");
        indexVecDoc2.setJsonEntity(vecDoc2);
        restClient.performRequest(indexVecDoc2);

        // Document 3: [3.0, 4.0, 0.0] - for distance calculations (magnitude = 5)
        String vecDoc3 = "{\n" +
                "  \"doc_id\": \"vec-3\",\n" +
                "  \"text\": \"neural networks\",\n" +
                "  \"embedding\": [3.0, 4.0, 0.0],\n" +
                "  \"vec_x\": 3.0,\n" +
                "  \"vec_y\": 4.0,\n" +
                "  \"vec_z\": 0.0\n" +
                "}";

        Request indexVecDoc3 = new Request("PUT", "/vector_docs/_doc/vec-3");
        indexVecDoc3.setJsonEntity(vecDoc3);
        restClient.performRequest(indexVecDoc3);

        // Refresh index
        Request refreshVectorIndex = new Request("POST", "/vector_docs/_refresh");
        restClient.performRequest(refreshVectorIndex);

        log.info("Inserted vector documents with array embeddings");
    }

    // ========== Nested Field Query Tests ==========

    @Test
    public void testSelectNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"token_usage.total_tokens\" FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT)");
    }

    @Test
    public void testSelectMultipleNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"token_usage.total_tokens\", \"token_usage.prompt_tokens\" " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT), CAST(45308 AS BIGINT)");
    }

    @Test
    public void testPredicateOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"token_usage.total_tokens\" > 50000 ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testDeeplyNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"reliability_scores.answer_relevance.score\" FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT 80");
    }

    @Test
    public void testDeeplyNestedFieldWithConfidence()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"reliability_scores.answer_relevance.score\", \"reliability_scores.answer_relevance.confidence\" " +
                        "FROM analytics_query_index WHERE _id = 'test-id-2'",
                "SELECT 90, 0.92");
    }

    @Test
    public void testAggregateOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id), AVG(\"token_usage.total_tokens\") FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT), 85205.33333333333");
    }

    @Test
    public void testGroupByNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(\"token_usage.total_tokens\") FROM analytics_query_index GROUP BY user_id ORDER BY user_id",
                "VALUES ('user1', CAST(271232 AS BIGINT)), ('user2', CAST(240000 AS BIGINT))");
    }

    @Test
    public void testOrderByNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id, \"token_usage.total_tokens\" FROM analytics_query_index " +
                        "ORDER BY \"token_usage.total_tokens\" DESC",
                "VALUES ('test-id-2', CAST(120000 AS BIGINT)), ('test-id-1', CAST(90616 AS BIGINT)), ('test-id-3', CAST(45000 AS BIGINT))");
    }

    @Test
    public void testMultipleNestedFieldsInPredicate()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 50000 " +
                        "AND \"reliability_scores.answer_relevance.score\" > 75 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testNestedFieldWithMetadata()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"metadata.model_name\", \"token_usage.total_tokens\" " +
                        "FROM analytics_query_index " +
                        "WHERE \"metadata.model_name\" = 'gpt-4' " +
                        "ORDER BY \"token_usage.total_tokens\"",
                "VALUES ('gpt-4', CAST(90616 AS BIGINT)), ('gpt-4', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testCountWithNestedFieldFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id) FROM analytics_query_index WHERE \"token_usage.total_tokens\" > 80000",
                "SELECT CAST(2 AS BIGINT)");
    }

    @Test
    public void testMaxMinOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT MAX(\"token_usage.total_tokens\"), MIN(\"token_usage.total_tokens\") FROM analytics_query_index",
                "SELECT CAST(120000 AS BIGINT), CAST(45000 AS BIGINT)");
    }

    // ========== Vector Search Function Tests Using Array Column from OpenSearch Index ==========

    @Test
    public void testVectorMagnitudeFromArrayColumn()
    {
        // Test magnitude using embedding array column directly from index
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, vector_magnitude(embedding) as magnitude " +
                        "FROM vector_docs WHERE doc_id = 'vec-3'",
                "VALUES ('vec-3', 5.0)");
    }

    @Test
    public void testVectorCosineSimilarityFromArrayColumn()
    {
        // Test cosine similarity using embedding array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "vector_cosine_similarity(embedding, ARRAY[1.0, 0.0, 0.0]) as similarity " +
                        "FROM vector_docs " +
                        "ORDER BY similarity DESC",
                "VALUES ('vec-1', 1.0), ('vec-3', 0.6), ('vec-2', 0.0)");
    }

    @Test
    public void testVectorEuclideanDistanceFromArrayColumn()
    {
        // Test Euclidean distance using embedding array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "vector_euclidean_distance(ARRAY[0.0, 0.0, 0.0], embedding) as distance " +
                        "FROM vector_docs WHERE doc_id = 'vec-3'",
                "VALUES ('vec-3', 5.0)");
    }

    @Test
    public void testVectorDotProductFromArrayColumn()
    {
        // Test dot product using embedding array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "vector_dot_product(embedding, ARRAY[1.0, 1.0, 0.0]) as score " +
                        "FROM vector_docs " +
                        "ORDER BY score DESC",
                "VALUES ('vec-3', 7.0), ('vec-1', 1.0), ('vec-2', 1.0)");
    }

    @Test
    public void testVectorSearchWithArrayColumnFilter()
    {
        // Filter documents using vector functions on array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM vector_docs " +
                        "WHERE vector_magnitude(embedding) > 1.5 " +
                        "ORDER BY doc_id",
                "VALUES ('vec-3')");
    }
}
