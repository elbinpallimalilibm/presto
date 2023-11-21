package com.facebook.presto.spi.error;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public class ErrorKeyStruct
{
    private final String name;

    @JsonCreator
    @ThriftConstructor
    public ErrorKeyStruct(@JsonProperty("name") String name)
    {
        this.name = name;
    }

    @JsonProperty
    @ThriftField(1)
    public String getName()
    {
        return name;
    }
}
