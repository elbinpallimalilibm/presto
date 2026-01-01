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
package com.facebook.presto.plugin.opensearch.functions;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;

/**
 * Vector similarity functions for OpenSearch connector.
 * These functions can be used in SQL queries to calculate
 * similarity between vectors.
 */
public final class VectorFunctions
{
    private VectorFunctions() {}

    @ScalarFunction("vector_cosine_similarity")
    @Description("Calculates cosine similarity between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    public static double vectorCosineSimilarity(
            @SqlType("array(real)") Block vector1Block,
            @SqlType("array(real)") Block vector2Block)
    {
        int length1 = vector1Block.getPositionCount();
        int length2 = vector2Block.getPositionCount();

        if (length1 != length2) {
            throw new IllegalArgumentException(
                    String.format("Vector dimensions must match: %d vs %d", length1, length2));
        }

        if (length1 == 0) {
            throw new IllegalArgumentException("Vectors cannot be empty");
        }

        float[] vector1 = new float[length1];
        float[] vector2 = new float[length2];

        for (int i = 0; i < length1; i++) {
            vector1[i] = Float.intBitsToFloat((int) vector1Block.getInt(i));
            vector2[i] = Float.intBitsToFloat((int) vector2Block.getInt(i));
        }

        return cosineSimilarity(vector1, vector2);
    }

    @ScalarFunction("vector_euclidean_distance")
    @Description("Calculates Euclidean distance between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    public static double vectorEuclideanDistance(
            @SqlType("array(real)") Block vector1Block,
            @SqlType("array(real)") Block vector2Block)
    {
        int length1 = vector1Block.getPositionCount();
        int length2 = vector2Block.getPositionCount();

        if (length1 != length2) {
            throw new IllegalArgumentException(
                    String.format("Vector dimensions must match: %d vs %d", length1, length2));
        }

        if (length1 == 0) {
            throw new IllegalArgumentException("Vectors cannot be empty");
        }

        float[] vector1 = new float[length1];
        float[] vector2 = new float[length2];

        for (int i = 0; i < length1; i++) {
            vector1[i] = Float.intBitsToFloat((int) vector1Block.getInt(i));
            vector2[i] = Float.intBitsToFloat((int) vector2Block.getInt(i));
        }

        return euclideanDistance(vector1, vector2);
    }

    @ScalarFunction("vector_dot_product")
    @Description("Calculates dot product between two vectors")
    @SqlType(StandardTypes.DOUBLE)
    public static double vectorDotProduct(
            @SqlType("array(real)") Block vector1Block,
            @SqlType("array(real)") Block vector2Block)
    {
        int length1 = vector1Block.getPositionCount();
        int length2 = vector2Block.getPositionCount();

        if (length1 != length2) {
            throw new IllegalArgumentException(
                    String.format("Vector dimensions must match: %d vs %d", length1, length2));
        }

        if (length1 == 0) {
            throw new IllegalArgumentException("Vectors cannot be empty");
        }

        float[] vector1 = new float[length1];
        float[] vector2 = new float[length2];

        for (int i = 0; i < length1; i++) {
            vector1[i] = Float.intBitsToFloat((int) vector1Block.getInt(i));
            vector2[i] = Float.intBitsToFloat((int) vector2Block.getInt(i));
        }

        return dotProduct(vector1, vector2);
    }

    @ScalarFunction("vector_magnitude")
    @Description("Calculates the magnitude (L2 norm) of a vector")
    @SqlType(StandardTypes.DOUBLE)
    public static double vectorMagnitude(@SqlType("array(real)") Block vectorBlock)
    {
        int length = vectorBlock.getPositionCount();

        if (length == 0) {
            throw new IllegalArgumentException("Vector cannot be empty");
        }

        float[] vector = new float[length];
        for (int i = 0; i < length; i++) {
            vector[i] = Float.intBitsToFloat((int) vectorBlock.getInt(i));
        }

        double sum = 0.0;
        for (float v : vector) {
            sum += v * v;
        }

        return Math.sqrt(sum);
    }

    // Helper methods

    private static double cosineSimilarity(float[] vector1, float[] vector2)
    {
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

    private static double euclideanDistance(float[] vector1, float[] vector2)
    {
        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }

        return Math.sqrt(sum);
    }

    private static double dotProduct(float[] vector1, float[] vector2)
    {
        double result = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            result += vector1[i] * vector2[i];
        }

        return result;
    }
}

// Made with Bob
