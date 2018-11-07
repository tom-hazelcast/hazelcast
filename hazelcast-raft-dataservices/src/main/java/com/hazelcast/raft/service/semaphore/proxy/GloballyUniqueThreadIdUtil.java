/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.raft.service.semaphore.proxy;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ThreadUtil.getThreadId;

/**
 * TODO: Javadoc Pending...
 */
public final class GloballyUniqueThreadIdUtil {

    public static final String GLOBAL_THREAD_ID_GENERATOR_NAME = "globalThreadIdGenerator";

    private static final ConcurrentMap<Tuple2<RaftGroupId, Long>, Long> globalThreadIds
            = new ConcurrentHashMap<Tuple2<RaftGroupId, Long>, Long>();

    public static Long getGlobalThreadId(RaftGroupId groupId, ConstructorFunction<RaftGroupId, Long> ctorFunction) {
        Tuple2<RaftGroupId, Long> key = Tuple2.of(groupId, getThreadId());
        Long globalThreadId = globalThreadIds.get(key);
        if (globalThreadId != null) {
            return globalThreadId;
        }

        globalThreadId = globalThreadIds.get(key);
        if (globalThreadId != null) {
            return globalThreadId;
        }

        globalThreadId = ctorFunction.createNew(groupId);
        Long existing = globalThreadIds.putIfAbsent(key, globalThreadId);

        return existing != null ? existing : globalThreadId;
    }

}