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
    private final String message;
    private final List<String> args;

    @JsonCreator
    @ThriftConstructor
    public ErrorKeyStruct(@JsonProperty("message") String message, @JsonProperty("args") List<String> args)
    {
        this.args = args;
        this.message = message;
    }

    @JsonProperty
    @ThriftField(1)
    public String getMessage()
    {
        return message;
    }

    @JsonProperty
    @ThriftField(2)
    public List<String> getArgs()
    {
        return args;
    }
}
