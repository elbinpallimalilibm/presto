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
package com.facebook.presto.spi.error;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@ThriftStruct
public class ErrorKeyStruct
{
    private final String key;
    private final List<List<Byte>> args;

    @JsonCreator
    @ThriftConstructor
    public ErrorKeyStruct(@JsonProperty("key") String key, @JsonProperty("args") List<List<Byte>> args)
    {
        this.args = args;
        this.key = key;
    }

    @JsonProperty
    @ThriftField(1)
    public String getKey()
    {
        return key;
    }

    @JsonProperty
    @ThriftField(2)
    public List<List<Byte>> getArgs()
    {
        return args;
    }
}
