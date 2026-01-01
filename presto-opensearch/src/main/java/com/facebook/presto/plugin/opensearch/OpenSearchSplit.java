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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Split for OpenSearch connector.
 * Represents a shard or partition of an OpenSearch index.
 */
public class OpenSearchSplit
        implements ConnectorSplit
{
    private final String indexName;
    private final int shardId;
    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public OpenSearchSplit(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("shardId") int shardId,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain)
    {
        this.indexName = requireNonNull(indexName, "indexName is null");
        this.shardId = shardId;
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
    }

    public OpenSearchSplit(String indexName, int shardId, List<HostAddress> addresses)
    {
        this(indexName, shardId, addresses, TupleDomain.all());
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
    }

    @JsonProperty
    public int getShardId()
    {
        return shardId;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        OpenSearchSplit other = (OpenSearchSplit) obj;
        return Objects.equals(indexName, other.indexName) &&
                shardId == other.shardId &&
                Objects.equals(addresses, other.addresses) &&
                Objects.equals(tupleDomain, other.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, shardId, addresses, tupleDomain);
    }

    @Override
    public String toString()
    {
        return indexName + "[" + shardId + "] @ " +
                addresses.stream()
                        .map(HostAddress::toString)
                        .collect(toImmutableList());
    }
}

// Made with Bob
