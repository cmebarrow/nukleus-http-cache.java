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
package org.reaktivity.nukleus.http_cache.internal;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class HttpCacheCounters
{
    public final Function<String, LongSupplier> supplyCounter;
    public final Function<String, LongConsumer> supplyAccumulator;

    public final LongSupplier requests;
    public final LongSupplier requestsCacheable;
    public final LongSupplier requestsPreferWait;
    public final LongSupplier requestsRetry;
    public final LongSupplier responses;
    public final LongSupplier responsesRetry;
    public final LongSupplier responsesCached;
    public final LongSupplier responsesAbortedPurge;
    public final LongSupplier responsesAbortedVary;
    public final LongSupplier responsesAbortedMiss;
    public final LongSupplier responsesAbortedEvicted;
    public final LongSupplier responsesAbortedUncommited;
    public final LongSupplier promises;
    public final LongSupplier promisesCanceled;

    public HttpCacheCounters(
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.supplyCounter = supplyCounter;
        this.supplyAccumulator = supplyAccumulator;

        this.requests = supplyCounter.apply("requests");
        this.requestsCacheable = supplyCounter.apply("requests.cacheable");
        this.requestsPreferWait = supplyCounter.apply("requests.prefer.wait");
        this.requestsRetry = supplyCounter.apply("requests.retry");
        this.responses = supplyCounter.apply("responses");
        this.responsesRetry = supplyCounter.apply("responses.retry");
        this.responsesCached = supplyCounter.apply("responses.cached");
        this.responsesAbortedVary = supplyCounter.apply("responses.aborted.vary");
        this.responsesAbortedMiss = supplyCounter.apply("responses.aborted.miss");
        this.responsesAbortedEvicted = supplyCounter.apply("responses.aborted.evicted");
        this.responsesAbortedUncommited = supplyCounter.apply("responses.aborted.uncommited");
        this.responsesAbortedPurge = supplyCounter.apply("responses.aborted.purge");
        this.promises = supplyCounter.apply("promises");
        this.promisesCanceled = supplyCounter.apply("promises.canceled");
    }
}
