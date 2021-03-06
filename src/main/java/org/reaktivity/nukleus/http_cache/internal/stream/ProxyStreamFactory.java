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
package org.reaktivity.nukleus.http_cache.internal.stream;

import static java.util.Objects.requireNonNull;

import java.util.Random;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheCounters;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.Cache;
import org.reaktivity.nukleus.http_cache.internal.proxy.cache.CacheControl;
import org.reaktivity.nukleus.http_cache.internal.proxy.request.Request;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CountingBufferPool;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{
    final BeginFW beginRO = new BeginFW();
    final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    private final RouteFW routeRO = new RouteFW();

    final WindowFW windowRO = new WindowFW();

    final RouteManager router;
    final BudgetManager budgetManager;

    final LongSupplier supplyStreamId;
    final BufferPool requestBufferPool;
    final BufferPool responseBufferPool;
    final Long2ObjectHashMap<Request> correlations;
    final LongObjectBiConsumer<Runnable> scheduler;
    final LongSupplier supplyCorrelationId;
    final Supplier<String> supplyEtag;

    final Writer writer;
    final CacheControl cacheControlParser = new CacheControl();
    final Cache cache;
    final Random random;
    final HttpCacheCounters counters;

    public ProxyStreamFactory(
        RouteManager router,
        BudgetManager budgetManager,
        MutableDirectBuffer writeBuffer,
        BufferPool requestBufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Request> correlations,
        LongObjectBiConsumer<Runnable> scheduler,
        Cache cache,
        Supplier<String> supplyEtag,
        HttpCacheCounters counters)
    {
        this.supplyEtag = supplyEtag;
        this.router = requireNonNull(router);
        this.budgetManager = requireNonNull(budgetManager);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.requestBufferPool = new CountingBufferPool(
                requestBufferPool,
                counters.supplyCounter.apply("initial.request.acquires"),
                counters.supplyCounter.apply("initial.request.releases"));
        this.responseBufferPool = new CountingBufferPool(
                requestBufferPool,
                counters.supplyCounter.apply("response.acquires"),
                counters.supplyCounter.apply("response.releases"));
        this.correlations = requireNonNull(correlations);
        this.scheduler = scheduler;
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.cache = cache;

        this.writer = new Writer(writeBuffer);
        this.random = new Random();
        this.counters = counters;
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer networkThrottle)
    {
        final long networkRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return networkRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ProxyAcceptStream(this, networkThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ProxyConnectReplyStream(this, throttle, throttleId)::handleStream;
    }


    RouteFW resolveTarget(
            long sourceRef,
            long authorization,
            String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() && sourceName.equals(route.source().asString());
        };

        return router.resolve(authorization, filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

}
