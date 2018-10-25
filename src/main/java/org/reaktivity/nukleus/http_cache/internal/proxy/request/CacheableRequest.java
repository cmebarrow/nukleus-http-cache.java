/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.ETAG;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeaderOrDefault;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
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
    private BufferPool requestPool;
    private int requestSlot;
    private BufferPool responsePool;
    private IntArrayList responseSlots = new IntArrayList();
    private static final int NUM_OF_HEADER_SLOTS = 1;
    private int responseHeadersSize = 0;
    private int responseSize = 0;

    final MessageConsumer connect;
    final long connectRef;
    final LongSupplier supplyCorrelationId;
    final LongSupplier supplyStreamId;
    CacheState state;
    private int attempts;
    private String recentAuthorizationHeader;

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
        BufferPool bufferPool,
        int requestSlot,
        RouteManager router,
        boolean authorizationHeader,
        long authorization,
        short authScope,
        String etag)
    {
        super(acceptName,
              acceptReply,
              acceptReplyStreamId,
              acceptCorrelationId,
              router,
              requestURLHash,
              authorizationHeader,
              authorization,
              authScope,
              etag);
        this.state = CacheState.COMMITING;
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyStreamId = supplyStreamId;
        this.requestPool = bufferPool;
        this.requestSlot = requestSlot;

        this.connect = connect;
        this.connectRef = connectRef;
    }

    // TODO remove need for duplication
    public void copyRequestTo(
            MutableDirectBuffer buffer)
    {
        MutableDirectBuffer requestBuffer = requestPool.buffer(requestSlot());
        requestBuffer.getBytes(0, buffer, 0, requestBuffer.capacity());
    }

    public boolean storeResponseHeaders(
            ListFW<HttpHeaderFW> responseHeaders,
            Cache cache,
            BufferPool bp)
    {
        responsePool = bp;
        etag(getHeaderOrDefault(responseHeaders, ETAG, etag()));

        final int slotCapacity = bp.slotCapacity();
        if (slotCapacity < responseHeaders.sizeof())
        {
            return false;
        }
        int headerSlot = bp.acquire(this.etag().hashCode());
        if (headerSlot == Slab.NO_SLOT)
        {
            return false;
        }
        responseSlots.add(headerSlot);

        MutableDirectBuffer buffer = bp.buffer(headerSlot);
        buffer.putBytes(0, responseHeaders.buffer(), responseHeaders.offset(), responseHeaders.sizeof());
        this.responseHeadersSize = responseHeaders.sizeof();

        return true;
    }

    public boolean storeResponseData(
        Cache cache,
        DataFW data,
        BufferPool cacheBufferPool)
    {
        if (state == CacheState.COMMITING)
        {
            return storeResponseData(cache, cacheBufferPool, data.payload());
        }
        return false;
    }

    public boolean cache(EndFW end, Cache cache)
    {
        if (state == CacheState.COMMITING)
        {
            state = CacheState.COMMITTED;
            boolean copied = moveDataToCachePools(cache.cachedRequestBufferPool, cache.cachedResponseBufferPool);
            if (copied)
            {
                cache.put(requestURLHash(), this);
            }
            return copied;
        }
        return false;
    }

    public void purge()
    {
        if (state != CacheState.PURGED)
        {
            if (requestSlot != Slab.NO_SLOT)
            {
                requestPool.release(requestSlot);
                this.requestSlot = Slab.NO_SLOT;
            }
            this.responseSlots.forEach(i -> responsePool.release(i));
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

    public final int requestSlot()
    {
        return requestSlot;
    }

    public final ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO)
    {
        return getRequestHeaders(requestHeadersRO, requestPool);
    }

    public final ListFW<HttpHeaderFW> getRequestHeaders(
        ListFW<HttpHeaderFW> requestHeadersRO,
        BufferPool bp)
    {
        final MutableDirectBuffer buffer = bp.buffer(requestSlot);
        return requestHeadersRO.wrap(buffer, 0, buffer.capacity());
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO)
    {
        return getResponseHeaders(responseHeadersRO, responsePool);
    }

    public ListFW<HttpHeaderFW> getResponseHeaders(
        ListFW<HttpHeaderFW> responseHeadersRO,
        BufferPool bp)
    {
        Integer firstResponseSlot = responseSlots.get(0);
        MutableDirectBuffer responseBuffer = bp.buffer(firstResponseSlot);
        return responseHeadersRO.wrap(responseBuffer, 0, responseHeadersSize);
    }

    public MessageConsumer connect()
    {
        return connect;
    }

    private boolean storeResponseData(
        Cache cache,
        BufferPool bp,
        Flyweight data)
    {
        return this.storeResponseData(cache, bp, data, 0);
    }

    private boolean storeResponseData(
        Cache cache,
        BufferPool bp,
        Flyweight data,
        int written)
    {
        responsePool = bp;
        if (data.sizeof() - written == 0)
        {
            return true;
        }

        final int slotCapacity = bp.slotCapacity();
        int slotSpaceRemaining = (slotCapacity * (responseSlots.size() - NUM_OF_HEADER_SLOTS)) - responseSize;
        if (slotSpaceRemaining == 0)
        {
            slotSpaceRemaining = slotCapacity;
            int newSlot = bp.acquire(this.etag().hashCode());
            if (newSlot == Slab.NO_SLOT)
            {
                return false;
            }
            responseSlots.add(newSlot);
        }

        int toWrite = Math.min(slotSpaceRemaining, data.sizeof() - written);

        int slot = responseSlots.get(responseSlots.size() - NUM_OF_HEADER_SLOTS);

        MutableDirectBuffer buffer = bp.buffer(slot);
        buffer.putBytes(slotCapacity - slotSpaceRemaining, data.buffer(), data.offset() + written, toWrite);
        written += toWrite;
        responseSize += toWrite;
        return storeResponseData(cache, bp, data, written);
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
            assert buffer1 != buffer2;
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

    private boolean moveDataToCachePools(BufferPool cachedRequestBufferPool, BufferPool cachedResponseBufferPool)
    {
        int id = etag().hashCode();
        int cachedRequestSlot = NO_SLOT;

        if (requestSlot != NO_SLOT)
        {
            cachedRequestSlot = copy(id, requestSlot, requestPool, cachedRequestBufferPool);
            if (cachedRequestSlot == NO_SLOT)
            {
                return false;
            }

            requestPool.release(requestSlot);
        }

        IntArrayList cachedResponseSlots = null;
        if (responseSlots != null)
        {
            cachedResponseSlots = new IntArrayList();
            for (int slot : responseSlots)
            {
                int cachedResponseSlot = copy(id, slot, responsePool, cachedResponseBufferPool);
                if (cachedResponseSlot == NO_SLOT)
                {
                    cachedRequestBufferPool.release(cachedRequestSlot);

                    for (int responseSlot : cachedResponseSlots)
                    {
                        cachedResponseBufferPool.release(responseSlot);
                    }
                    return false;
                }
                else
                {
                    cachedResponseSlots.add(cachedResponseSlot);
                }
            }

            this.responseSlots.forEach(i -> responsePool.release(i));
        }

        this.requestSlot = cachedRequestSlot;
        this.requestPool = cachedRequestBufferPool;
        this.responseSlots = cachedResponseSlots;
        this.responsePool = cachedResponseBufferPool;
        return true;
    }

    private static int copy(int id, int fromSlot, BufferPool fromBP, BufferPool toBP)
    {
        int toSlot = toBP.acquire(0);
        // should we purge old cache entries ?
        if (toSlot != NO_SLOT)
        {
            DirectBuffer fromBuffer = fromBP.buffer(fromSlot);
            MutableDirectBuffer toBuffer = toBP.buffer(toSlot);
            toBuffer.putBytes(0, fromBuffer, 0, fromBuffer.capacity());
        }

        return toSlot;
    }

    public void incAttempts()
    {
        attempts++;
    }

    public int attempts()
    {
        return attempts;
    }

    public String recentAuthorizationHeader()
    {
        return recentAuthorizationHeader;
    }

    public void recentAuthorizationHeader(String recentAuthorizationHeader)
    {
        this.recentAuthorizationHeader = recentAuthorizationHeader;
    }

}
