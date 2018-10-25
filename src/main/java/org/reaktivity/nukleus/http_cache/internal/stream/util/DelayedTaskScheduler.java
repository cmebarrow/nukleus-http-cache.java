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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.reaktivity.nukleus.Nukleus;

public class DelayedTaskScheduler implements Nukleus
{
    private final Long2ObjectHashMap<Runnable> taskLookup;
    private final SortedSet<Long> scheduledTimes;

    private boolean schedulingDeferred;
    private final LongArrayList deferredTimes;
    private final ArrayList<Runnable> deferredTasks;

    public DelayedTaskScheduler()
    {
        this.taskLookup = new Long2ObjectHashMap<>();
        this.scheduledTimes = new TreeSet<>();
        this.deferredTimes = new LongArrayList();
        this.deferredTasks = new ArrayList<>();
    }

    public void schedule(long time, Runnable task)
    {
        if (schedulingDeferred)
        {
            deferredTimes.add(time);
            deferredTasks.add(task);
        }
        else
        {
            scheduleTask(time, task);
        }
    }

    private void scheduleTask(long time, Runnable task)
    {
        if (this.scheduledTimes.add(time))
        {
            this.taskLookup.put(time, task);
        }
        else
        {
            this.taskLookup.merge(time, task, mergeTasks);
        }
    }

    public int process()
    {
        int work = 0;
        schedulingDeferred = true;
        if (!scheduledTimes.isEmpty())
        {
            long c = System.currentTimeMillis();
            SortedSet<Long> past = scheduledTimes.headSet(c);
            Iterator<Long> iter = past.iterator();
            while(iter.hasNext())
            {
                Long time = iter.next();
                iter.remove();
                taskLookup.remove(time).run();
            }
            work = past.size();
        }
        schedulingDeferred = false;

        // Schedule all the tasks that were deferred during the execution of expired tasks
        if (!deferredTimes.isEmpty())
        {
            Iterator<Runnable> taskIt = deferredTasks.iterator();
            deferredTimes.forEachOrderedLong(time -> scheduleTask(time, taskIt.next()));
            deferredTimes.clear();
            deferredTasks.clear();
        }

        return work;
    }

    private static BiFunction<? super Runnable, ? super Runnable, ? extends Runnable> mergeTasks = (t1, t2) ->
    {
        return () ->
        {
            t1.run();
            t2.run();
        };
    };
}
