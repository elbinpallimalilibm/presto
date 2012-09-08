/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static com.facebook.presto.Blocks.assertBlockStreamEquals;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestUncompressedBlockReader
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(Type.VARIABLE_BINARY);
        UncompressedValueBlock block = new BlockBuilder(0, tupleInfo)
                .append("alice".getBytes(UTF_8))
                .append("bob".getBytes(UTF_8))
                .append("charlie".getBytes(UTF_8))
                .append("dave".getBytes(UTF_8))
                .build();

        UncompressedBlockStream blockStream = new UncompressedBlockStream(tupleInfo, ImmutableList.of(block));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ColumnProcessor processor = new UncompressedColumnWriter(Type.VARIABLE_BINARY, 0, blockStream.cursor(), out);
        processor.processPositions(Integer.MAX_VALUE);
        processor.finish();

        ImmutableList<UncompressedValueBlock> copiedBlocks = ImmutableList.copyOf(UncompressedBlockSerde.read(Slices.wrappedBuffer(out.toByteArray())));

        // this is only true because the input is small
        assertEquals(copiedBlocks.size(), 1);
        ValueBlock copiedBlock = copiedBlocks.get(0);
        assertEquals(copiedBlock.getRange(), block.getRange());

        assertBlockStreamEquals(new UncompressedBlockStream(tupleInfo, copiedBlocks), blockStream);
    }
}
