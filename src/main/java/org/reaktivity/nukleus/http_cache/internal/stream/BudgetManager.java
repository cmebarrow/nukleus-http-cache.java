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
package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.function.IntUnaryOperator;

import static java.util.Objects.requireNonNull;

public class BudgetManager
{
    private final Long2ObjectHashMap<GroupBudget> groups;             // group id -> GroupBudget

    public enum StreamKind
    {
        CACHE,
        PROXY
    }

    private static class StreamBudget
    {
        final long streamId;
        final StreamKind streamKind;
        int unackedBudget;
        int index;
        IntUnaryOperator budgetAvailable;
        boolean closing;

        StreamBudget(long streamId, StreamKind kind, IntUnaryOperator budgetAvailable, int index)
        {
            this.streamId = streamId;
            this.streamKind = requireNonNull(kind);
            this.budgetAvailable = budgetAvailable;
            this.index = index;
        }

        @Override
        public String toString()
        {
            return String.format("(id=%d kind=%s closing=%s unackedBudget=%d)", streamId, streamKind, closing, unackedBudget);
        }
    }

    private class GroupBudget
    {
        final long groupId;
        final int initialBudget;
        final Long2ObjectHashMap<StreamBudget> streamMap;          // stream id -> BudgetEnty
        int budget;

        GroupBudget(long groupId, int initialBudget)
        {
            this.groupId = groupId;
            this.initialBudget = initialBudget;
            this.streamMap = new Long2ObjectHashMap<>();
        }

        void add(long streamId, StreamBudget streamBudget)
        {
            streamMap.put(streamId, streamBudget);
        }

        StreamBudget get(long streamId)
        {
            return streamMap.get(streamId);
        }

        boolean isEmpty()
        {
            return streamMap.isEmpty();
        }

        int size()
        {
            return streamMap.size();
        }

        StreamBudget remove(long streamId)
        {
            return streamMap.remove(streamId);
        }

        private void moreBudget(int credit)
        {
            budget += credit;
            assert budget <= initialBudget;

            streamMap.forEach((k, stream) ->
            {
                // Give budget to first stream. TODO fairness
                if (!stream.closing && budget > 0)
                {
                    int slice = budget;
                    budget -= slice;
                    stream.unackedBudget += slice;
                    int remaining = stream.budgetAvailable.applyAsInt(slice);
                    budget += remaining;
                    stream.unackedBudget -= remaining;
                }
            });
        }

        @Override
        public String toString()
        {
            long proxyStreams = streamMap.values().stream().filter(s -> s.streamKind == StreamKind.PROXY).count();
            long cacheStreams = streamMap.values().stream().filter(s -> s.streamKind == StreamKind.CACHE).count();
            long unackedStreams = streamMap.values().stream().filter(s -> s.unackedBudget > 0).count();

            return String.format("(groupId=%d budget=%d proxyStreams=%d cacheStreams=%d unackedStreams=%d)",
                    groupId, budget, proxyStreams, cacheStreams, unackedStreams);
        }
    }

    BudgetManager()
    {
        groups = new Long2ObjectHashMap<>();
    }

    void closing(long groupId, long streamId, int credit)
    {
        if (groupId != 0)
        {
            GroupBudget groupBudget = groups.get(groupId);
            StreamBudget streamBudget = groupBudget.get(streamId);
            streamBudget.unackedBudget -= credit;
            streamBudget.closing = true;
            if (credit > 0)
            {
                groupBudget.moreBudget(credit);
            }
        }
    }

    public void closed(StreamKind streamKind, long groupId, long streamId)
    {
        if (groupId != 0)
        {
            GroupBudget groupBudget = groups.get(groupId);
            if (groupBudget != null)
            {
                StreamBudget streamBudget = groupBudget.remove(streamId);
                if (groupBudget.isEmpty())
                {
                    groups.remove(groupId);
                }
                else if (streamBudget != null && streamBudget.unackedBudget > 0)
                {
                    groupBudget.moreBudget(streamBudget.unackedBudget);
                }
            }
        }
    }

    public void window(StreamKind streamKind, long groupId, long streamId, int credit, IntUnaryOperator budgetAvailable)
    {
        if (groupId == 0)
        {
            budgetAvailable.applyAsInt(credit);
        }
        else
        {
            boolean gotBudget = false;
            GroupBudget groupBudget = groups.get(groupId);
            if (groupBudget == null)
            {
                groupBudget = new GroupBudget(groupId, credit);
                groups.put(groupId, groupBudget);
                gotBudget = true;
            }
            StreamBudget streamBudget = groupBudget.get(streamId);
            if (streamBudget == null)
            {
                // Ignore initial window of a stream (except the very first stream)
                int index = groupBudget.size();
                streamBudget = new StreamBudget(streamId, streamKind, budgetAvailable, index);
                groupBudget.add(streamId, streamBudget);
            }
            else
            {
                streamBudget.unackedBudget -= credit;
                assert streamBudget.unackedBudget >= 0;
                gotBudget = true;
            }

            if (gotBudget && credit > 0)
            {
                groupBudget.moreBudget(credit);
            }
        }
    }

    public boolean hasUnackedBudget(long groupId, long streamId)
    {
        if (groupId == 0)
        {
            return false;
        }
        else
        {
            GroupBudget groupBudget = groups.get(groupId);
            StreamBudget streamBudget = groupBudget.get(streamId);
            return streamBudget.unackedBudget != 0;
        }
    }
}
