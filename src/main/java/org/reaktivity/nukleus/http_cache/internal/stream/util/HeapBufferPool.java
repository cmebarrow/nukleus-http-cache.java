/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.reaktivity.nukleus.http_cache.internal.stream.util;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;

import java.nio.ByteBuffer;

public class HeapBufferPool implements BufferPool
{
    final MutableDirectBuffer[] buffers;
    final int slotCapacity;

    public HeapBufferPool(
        int slotCount,
        int slotCapacity)
    {
        buffers = new UnsafeBuffer[slotCount];
        this.slotCapacity = slotCapacity;
    }

    HeapBufferPool(
        MutableDirectBuffer[] buffers,
        int slotCapacity)
    {
        this.buffers = buffers;
        this.slotCapacity = slotCapacity;
    }

    @Override
    public int slotCapacity()
    {
        return slotCapacity;
    }

    @Override
    public int acquire(long streamId)
    {
        for(int i=0; i < buffers.length; i++)
        {
            if (buffers[i] == null)
            {
                buffers[i] = new UnsafeBuffer(new byte[slotCapacity]);
                return i;
            }
        }

        return NO_SLOT;
    }

    @Override
    public MutableDirectBuffer buffer(
        int slot)
    {
        assert buffers[slot] != null;
        return buffers[slot];
    }

    @Override
    public ByteBuffer byteBuffer(
        int slot)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableDirectBuffer buffer(
        int slot,
        int offset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release(int slot)
    {
        if (slot != NO_SLOT)
        {
            buffers[slot] = null;
        }
    }

    @Override
    public BufferPool duplicate()
    {
        return this;
    }

    @Override
    public int acquiredSlots()
    {
        throw new UnsupportedOperationException();
    }
}
