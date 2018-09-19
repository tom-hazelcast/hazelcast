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

package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.raft.RaftGroupId;

/**
 * TODO: Javadoc Pending...
 */
public class RaftAtomicLong {

    private final RaftGroupId groupId;
    private final String name;

    private long value;

    RaftAtomicLong(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    RaftAtomicLong(RaftGroupId groupId, String name, long value) {
        this.groupId = groupId;
        this.name = name;
        this.value = value;
    }

    public RaftGroupId groupId() {
        return groupId;
    }

    public String name() {
        return name;
    }

    public long addAndGet(long delta) {
        value += delta;
        return value;
    }

    public long getAndAdd(long delta) {
        long v = value;
        value += delta;
        return v;
    }

    public long getAndSet(long value) {
        long v = this.value;
        this.value = value;
        return v;
    }

    public boolean compareAndSet(long currentValue, long newValue) {
        if (value == currentValue) {
            value = newValue;
            return true;
        }
        return false;
    }

    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return "RaftAtomicLong{" + "groupId=" + groupId + ", name='" + name + '\'' + ", value=" + value + '}';
    }
}