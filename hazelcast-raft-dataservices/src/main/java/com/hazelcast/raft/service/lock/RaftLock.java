/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.lock;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * TODO: Javadoc Pending...
 */
class RaftLock extends BlockingResource<LockInvocationKey> {

    private LockInvocationKey owner;
    private int lockCount;
    private UUID releaseRefUid;

    RaftLock(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    RaftLock(RaftLockSnapshot snapshot) {
        super(snapshot.getGroupId(), snapshot.getName());
        this.owner = snapshot.getOwner();
        this.lockCount = snapshot.getLockCount();
        this.releaseRefUid = snapshot.getRefUid();
        this.waitKeys.addAll(snapshot.getWaitEntries());
    }

    boolean acquire(LockEndpoint endpoint, long commitIndex, UUID invocationUid, boolean wait) {
        // if acquire() is being retried
        if (owner != null && owner.invocationUid().equals(invocationUid)) {
            return true;
        }

        LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
        if (owner == null) {
            owner = key;
        }

        if (endpoint.equals(owner.endpoint())) {
            lockCount++;
            return true;
        }

        if (wait) {
            waitKeys.offer(key);
        }

        return false;
    }

    Collection<LockInvocationKey> release(LockEndpoint endpoint, UUID invocationUuid) {
        return release(endpoint, 1, invocationUuid);
    }

    private Collection<LockInvocationKey> release(LockEndpoint endpoint, int releaseCount, UUID invocationUid) {
        // if release() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner != null && endpoint.equals(owner.endpoint())) {
            releaseRefUid = invocationUid;

            lockCount -= Math.min(releaseCount, lockCount);
            if (lockCount > 0) {
                return Collections.emptyList();
            }

            LockInvocationKey nextOwner = waitKeys.poll();
            if (nextOwner != null) {
                List<LockInvocationKey> entries = new ArrayList<LockInvocationKey>();
                entries.add(nextOwner);

                Iterator<LockInvocationKey> iter = waitKeys.iterator();
                while (iter.hasNext()) {
                    LockInvocationKey n = iter.next();
                    if (nextOwner.invocationUid().equals(n.invocationUid())) {
                        iter.remove();
                        assert nextOwner.endpoint().equals(n.endpoint());
                        entries.add(n);
                    }
                }

                owner = nextOwner;
                lockCount = 1;
                return entries;
            } else {
                owner = null;
            }

            return Collections.emptyList();
        }

        throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
    }

    Collection<LockInvocationKey> forceRelease(long expectedFence, UUID invocationUid) {
        // if forceRelease() is being retried
        if (invocationUid.equals(releaseRefUid)) {
            return Collections.emptyList();
        }

        if (owner == null) {
            throw new IllegalMonitorStateException();
        }

        if (owner.commitIndex() == expectedFence) {
            return release(owner.endpoint(), lockCount, invocationUid);
        }

        throw new IllegalMonitorStateException();
    }

    int lockCount() {
        return lockCount;
    }

    LockInvocationKey owner() {
        return owner;
    }

    RaftLockSnapshot toSnapshot() {
        return new RaftLockSnapshot(groupId, name, owner, lockCount, releaseRefUid, waitKeys);
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> result) {
        if (owner != null && sessionId == owner.endpoint().sessionId()) {
            Collection<LockInvocationKey> w = release(owner.endpoint(), Integer.MAX_VALUE, UuidUtil.newUnsecureUUID());
            if (w.isEmpty()) {
                return;
            }
            Object newOwnerCommitIndex = w.iterator().next().commitIndex();
            for (LockInvocationKey waitEntry : w) {
                result.put(waitEntry.commitIndex(), newOwnerCommitIndex);
            }
        }
    }
}
