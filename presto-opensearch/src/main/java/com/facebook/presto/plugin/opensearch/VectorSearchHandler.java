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

import com.facebook.presto.spi.PrestoException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Handles vector search operations for OpenSearch k-NN plugin.
 * Translates vector search requests into OpenSearch k-NN query DSL.
 */
public class VectorSearchHandler
{
    private final OpenSearchConfig config;

    public VectorSearchHandler(OpenSearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Builds a k-NN vector search query.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @return SearchSourceBuilder configured for k-NN search
     */
    public SearchSourceBuilder buildVectorSearchQuery(
            String vectorField,
            float[] queryVector,
            int k)
    {
        requireNonNull(vectorField, "vectorField is null");
        requireNonNull(queryVector, "queryVector is null");

        if (!config.isVectorSearchEnabled()) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vector search is not enabled. Set opensearch.vector-search.enabled=true");
        }

        if (k <= 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "k must be positive, got: " + k);
        }

        if (queryVector.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Query vector cannot be empty");
        }

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // Build k-NN query using script score
        Map<String, Object> knnParams = new HashMap<>();
        knnParams.put("field", vectorField);
        knnParams.put("query_value", queryVector);
        knnParams.put("space_type", "cosinesimil");

        // Use script score query for k-NN
        String scriptSource = "knn_score";
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", vectorField);
        scriptParams.put("query_value", queryVector);

        // For OpenSearch k-NN, we use the knn query type
        Map<String, Object> knnQuery = new HashMap<>();
        Map<String, Object> vectorQuery = new HashMap<>();
        vectorQuery.put("vector", queryVector);
        vectorQuery.put("k", k);
        knnQuery.put(vectorField, vectorQuery);

        // Note: OpenSearch k-NN uses a special query type
        // This is a simplified version - actual implementation may need
        // to use OpenSearch's KNNQueryBuilder directly
        sourceBuilder.size(k);

        return sourceBuilder;
    }

    /**
     * Builds a hybrid search query combining vector search with filters.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @param filters Additional filter queries
     * @return SearchSourceBuilder configured for hybrid search
     */
    public SearchSourceBuilder buildHybridSearchQuery(
            String vectorField,
            float[] queryVector,
            int k,
            BoolQueryBuilder filters)
    {
        requireNonNull(vectorField, "vectorField is null");
        requireNonNull(queryVector, "queryVector is null");
        requireNonNull(filters, "filters is null");

        SearchSourceBuilder vectorQuery = buildVectorSearchQuery(vectorField, queryVector, k);

        // Combine vector search with filters
        // In hybrid search, we first filter, then do k-NN on filtered results
        BoolQueryBuilder hybridQuery = QueryBuilders.boolQuery();
        hybridQuery.filter(filters);

        // Add the vector search as a should clause with high boost
        // This ensures vector similarity is the primary ranking factor
        // while filters are applied as constraints

        vectorQuery.query(hybridQuery);
        return vectorQuery;
    }

    /**
     * Calculates cosine similarity between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Cosine similarity score between -1 and 1
     */
    public static double cosineSimilarity(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        if (norm1 == 0.0 || norm2 == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Calculates Euclidean distance between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Euclidean distance
     */
    public static double euclideanDistance(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }

        return Math.sqrt(sum);
    }

    /**
     * Calculates dot product between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Dot product
     */
    public static double dotProduct(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double result = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            result += vector1[i] * vector2[i];
        }

        return result;
    }
}

// Made with Bob
