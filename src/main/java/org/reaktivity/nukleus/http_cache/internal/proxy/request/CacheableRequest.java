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
package org.reaktivity.nukleus.http_cache.internal.proxy.request;

import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeaderOrDefault;

import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayList;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.DirectBufferUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Slab;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.route.RouteManager;

public abstract class CacheableRequest extends AnswerableByCacheRequest
{
    private IntArrayList responseSlots = new IntArrayList();
    private static final int NUM_OF_HEADER_SLOTS = 1;
    private int responseHeadersSize = 0;
    private int responseSize = 0;

    final MessageConsumer connect;
    final long connectRef;
    final LongSupplier supplyCorrelationId;
    final LongSupplier supplyStreamId;
    CacheState state;

    public enum CacheState
    {
        COMMITING, COMMITTED, PURGED
    }

    public CacheableRequest(
        String acceptName,
        MessageConsumer acceptReply,
        long acceptReplyStreamId,
        long acceptCorrelationId,
        MessageConsumer connect,
        long connectRef,
        LongSupplier supplyCorrelationId,
        LongSupplier supplyStreamId,
        int requestURLHash,
        int requestSlot,
        RouteManager router,
        short authScope,
        String etag)
    {
        super(acceptName,
              acceptReply,
              acceptReplyStreamId,
              acceptCorrelationId,
              router,
              requestSlot,
              requestURLHash,
              authScope,
              etag);
        this.state = CacheState.COMMITING;
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyStreamId = supplyStreamId;

        this.connect = connect;
        this.connectRef = connectRef;
    }

    // TODO remove need for duplication
    public void copyRequestTo(
            MutableDirectBuffer buffer,
            BufferPool readFromBufferPool)
    {
        MutableDirectBuffer requestBuffer = readFromBufferPool.buffer(requestSlot());
        requestBuffer.getBytes(0, buffer, 0, requestBuffer.capacity());
    }

    public boolean cache(
            ListFW<HttpHeaderFW> responseHeaders,
            Cache cache,
            BufferPool bp)
    {
        etag(getHeaderOrDefault(responseHeaders, ETAG, etag()));

        final int slotCapacity = bp.slotCapacity();
        if (slotCapacity < responseHeaders.sizeof())
        {
            return false;
        }
        int headerSlot = bp.acquire(this.etag().hashCode());
        while (headerSlot == Slab.NO_SLOT)
        {
            cache.purgeOld();
            headerSlot = bp.acquire(this.etag().hashCode());
        }
        responseSlots.add(headerSlot);

        MutableDirectBuffer buffer = bp.buffer(headerSlot);
        buffer.putBytes(0, responseHeaders.buffer(), responseHeaders.offset(), responseHeaders.sizeof());
        this.responseHeadersSize = responseHeaders.sizeof();

        cache.notifyUncommitted(this);
        return true;
    }

    public void cache(
        Cache cache,
        DataFW data,
        BufferPool cacheBufferPool)
    {
        if (state == CacheState.COMMITING)
        {
            putResponse(cache, cacheBufferPool, data.payload());
        }
    }

    public void cache(EndFW end, Cache cache)
    {
        if (state == CacheState.COMMITING)
        {
            state = CacheState.COMMITTED;
            cache.put(requestURLHash(), this);
        }
    }

    public void purge(BufferPool bp)
    {
        if (state != CacheState.PURGED)
        {
            super.purge(bp);
            this.responseSlots.stream().forEach(i -> bp.release(i));
            this.responseSlots = null;
            this.state = CacheState.PURGED;
        }
    }

    public long connectRef()
    {
        return connectRef;
    }

    public LongSupplier supplyCorrelationId()
    {
        return supplyCorrelationId;
    }

    public LongSupplier supplyStreamId()
    {
        return supplyStreamId;
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO,
        BufferPool cacheBufferPool)
    {
        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = cacheBufferPool.buffer(firstResponseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public MessageConsumer connect()
    {
        return connect;
    }

    private void putResponse(
        Cache cache,
        BufferPool bp,
        Flyweight data)
    {
        this.putResponseData(cache, bp, data, 0);
    }

    private void putResponseData(
        Cache cache,
        BufferPool bp,
        Flyweight data,
        int written)
    {

        if (data.sizeof() - written == 0)
        {
            return;
        }

        final int slotCapacity = bp.slotCapacity();
        int slotSpaceRemaining = (slotCapacity * (responseSlots.size() - NUM_OF_HEADER_SLOTS)) - responseSize;
        if (slotSpaceRemaining == 0)
        {
            slotSpaceRemaining = slotCapacity;
            int newSlot = bp.acquire(this.etag().hashCode());
            while (newSlot == Slab.NO_SLOT)
            {
                cache.purgeOld();
                if (this.state == CacheState.PURGED)
                {
                    return;
                }
                newSlot = bp.acquire(this.etag().hashCode());
            }
            responseSlots.add(newSlot);
        }

        int toWrite = Math.min(slotSpaceRemaining, data.sizeof() - written);

        int slot = responseSlots.get(responseSlots.size() - NUM_OF_HEADER_SLOTS);

        MutableDirectBuffer buffer = bp.buffer(slot);
        buffer.putBytes(slotCapacity - slotSpaceRemaining, data.buffer(), data.offset() + written, toWrite);
        written += toWrite;
        responseSize += toWrite;
        putResponseData(cache, bp, data, written);

    }

    public boolean payloadEquals(
        CacheableRequest that,
        BufferPool bp1,
        BufferPool bp2)
    {
        int read = 0;
        boolean match = this.responseSize == that.responseSize;
        for (int i = 1; match && i < this.responseSlots.size(); i++)
        {
            int length = Math.min(bp1.slotCapacity(), this.responseSize - read);
            MutableDirectBuffer buffer1 = bp1.buffer(this.responseSlots.get(i));
            MutableDirectBuffer buffer2 = bp2.buffer(that.responseSlots.get(i));
            match = DirectBufferUtil.equals(buffer1, 0, length, buffer2, 0, length);
        }
        return match;
    }

    public int responseSize()
    {
        return responseSize;
    }

    public void buildResponsePayload(
        int index,
        int length,
        Builder p,
        BufferPool bp)
    {
        final int slotCapacity = bp.slotCapacity();
        final int startSlot = Math.floorDiv(index, slotCapacity) + NUM_OF_HEADER_SLOTS;
        buildResponsePayload(index, length, p, bp, startSlot);
    }

    public void buildResponsePayload(
            int index,
            int length,
            Builder builder,
            BufferPool bp,
            int slotCnt)
    {
        if (length == 0)
        {
            return;
        }

        final int slotCapacity = bp.slotCapacity();
        int chunkedWrite = (slotCnt * slotCapacity) - index;
        int slot = this.responseSlots.get(slotCnt);
        if (chunkedWrite > 0)
        {
            MutableDirectBuffer buffer = bp.buffer(slot);
            int offset = slotCapacity - chunkedWrite;
            int chunkLength = Math.min(chunkedWrite, length);
            builder.put(buffer, offset, chunkLength);
            index += chunkLength;
            length -= chunkLength;
        }
        buildResponsePayload(index, length, builder, bp, ++slotCnt);
    }
}
