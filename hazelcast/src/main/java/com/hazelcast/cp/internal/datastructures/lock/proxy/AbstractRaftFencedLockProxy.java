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

package com.hazelcast.cp.internal.datastructures.lock.proxy;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.FencedLock;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.Clock;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements proxy methods for Raft-based {@link FencedLock} API.
 * Lock reentrancy is implemented locally.
 */
public abstract class AbstractRaftFencedLockProxy extends SessionAwareProxy implements FencedLock {

    protected final String proxyName;
    protected final String objectName;
    // thread id -> lock state
    private final ConcurrentMap<Long, LockState> lockStates = new ConcurrentHashMap<Long, LockState>();

    public AbstractRaftFencedLockProxy(AbstractProxySessionManager sessionManager, CPGroupId groupId, String proxyName,
                                       String objectName) {
        super(sessionManager, groupId);
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public void lock() {
        lockAndGetFence();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        long threadId = getThreadId();
        if (tryReentrantLock(threadId) != INVALID_FENCE) {
            return;
        }

        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            try {
                RaftLockOwnershipState ownership = doLock(sessionId, threadId, invocationUid).get();
                assert ownership.isLockedBy(sessionId, threadId);

                // initialize the local state with the lock count returned from the Raft group
                // since I might have performed some lock() calls that failed with operation timeout
                // on my side but actually committed on the Raft group.

                lockStates.put(threadId, new LockState(sessionId, ownership.getFence(), ownership.getLockCount()));
                return;
            } catch (Throwable t) {
                if (t instanceof SessionExpiredException) {
                    invalidateSession(sessionId);
                } else if (t instanceof InterruptedException) {
                    throw (InterruptedException) t;
                } else {
                    throw rethrow(t);
                }
            }
        }
    }

    @Override
    public final long lockAndGetFence() {
        long threadId = getThreadId();
        long fence = tryReentrantLock(threadId);
        if (fence != INVALID_FENCE) {
            return fence;
        }

        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            try {
                RaftLockOwnershipState ownership = doLock(sessionId, threadId, invocationUid).join();
                assert ownership.isLockedBy(sessionId, threadId);

                // initialize the local state with the lock count returned from the Raft group
                // since I might have performed some lock() calls that failed with operation timeout
                // on my side but actually committed on the Raft group.

                fence = ownership.getFence();
                lockStates.put(threadId, new LockState(sessionId, fence, ownership.getLockCount()));
                return fence;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLockAndGetFence() != INVALID_FENCE;
    }

    @Override
    public final long tryLockAndGetFence() {
        return tryLockAndGetFence(0, MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, @Nonnull TimeUnit unit) {
        return tryLockAndGetFence(time, unit) != INVALID_FENCE;
    }

    @Override
    public final long tryLockAndGetFence(long time, @Nonnull TimeUnit unit) {
        checkNotNull(unit);

        long threadId = getThreadId();
        long fence = tryReentrantLock(threadId);
        if (fence != INVALID_FENCE) {
            return fence;
        }

        UUID invocationUid = newUnsecureUUID();
        long timeoutMillis = Math.max(0, unit.toMillis(time));
        long start;
        for (;;) {
            start = Clock.currentTimeMillis();
            long sessionId = acquireSession();
            try {
                RaftLockOwnershipState ownership = doTryLock(sessionId, threadId, invocationUid, timeoutMillis).join();
                if (ownership.isLockedBy(sessionId, threadId)) {
                    // initialize the local state with the lock count returned from the Raft group
                    // since I might have performed some lock() calls that failed with operation timeout
                    // on my side but actually committed on the Raft group.

                    fence = ownership.getFence();
                    lockStates.put(threadId, new LockState(sessionId, fence, ownership.getLockCount()));
                    return fence;
                }

                releaseSession(sessionId);
                return INVALID_FENCE;
            } catch (WaitKeyCancelledException e) {
                releaseSession(sessionId);
                return INVALID_FENCE;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
                timeoutMillis -= (Clock.currentTimeMillis() - start);
                if (timeoutMillis <= 0) {
                    return INVALID_FENCE;
                }
            }
        }
    }

    @Override
    public final void unlock() {
        long sessionId = getSession();
        long threadId = getThreadId();
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            validateLocalLockState(sessionId, threadId, lockState);
            if (lockState.lockCount > 1) {
                lockState.lockCount--;
                return;
            }
        } else if (sessionId == NO_SESSION_ID) {
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + proxyName
                    + "] because session not found!");
        }

        // even if there is no local lock state, I still hit the Raft group because
        // it could have happened that I performed at least one lock() call
        // that have failed with operation timeout on my side
        // but actually committed on the Raft group.

        // consider the following scenario involving a single client, which is an interesting case:
        // 1. lock() -- fails with operation timeout locally but committed in the Raft group
        // 2. lock() -- fails with operation timeout locally but committed in the Raft group
        // 3. unlock(Integer.MAX_VALUE)
        // After the second step, my lock count is 2 in the Raft group, however I couldn't observe it
        // and actually no one else can also observe it because of the behaviour of the getLockCountIfLockedByCurrentThread()
        // method. In this case, the system will just pretend that I acquired the lock only once.
        // In the third step, I will release all of my acquires at once with the following step.
        // This behaviour implies that if multiple lock() calls are committed on the server
        // but failed with operation timeout on the client, they are not differentiable
        // from a single acquire until the lock owner observes them with another successful
        // lock(), getFence(), isLocked(), isLockedByCurrentThread(), or getLockCountIfLockedByCurrentThread() call.

        try {
            doUnlock(sessionId, threadId, newUnsecureUUID(), Integer.MAX_VALUE).join();
        } catch (SessionExpiredException e) {
            invalidateSession(sessionId);
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + proxyName + "] because Session["
                    + sessionId + "] is closed by server!");
        } finally {
            lockStates.remove(threadId);
            releaseSession(sessionId);
        }
    }

    @Override
    public final Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void forceUnlock() {
        try {
            RaftLockOwnershipState ownership = doGetLockOwnershipState().join();
            if (!ownership.isLocked()) {
                throw new IllegalMonitorStateException("Lock[" + objectName + "] has no owner!");
            }

            doForceUnlock(newUnsecureUUID(), ownership.getFence()).join();
        } finally {
            lockStates.remove(getThreadId());
        }
    }

    @Override
    public final long getFence() {
        long sessionId = getSession();
        long threadId = getThreadId();
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            validateLocalLockState(sessionId, threadId, lockState);
            return lockState.fence;
        } else if (sessionId == NO_SESSION_ID) {
            throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + proxyName
                    + "] because session not found!");
        }

        // If I learn from the response that I am the current lock owner, it means that
        // an earlier lock() request of mine failed with operation timeout on my side
        // but actually committed on the Raft group. In this case, I already acquired
        // the session before I made the failed lock() call, so there is no need to acquire it here.

        RaftLockOwnershipState ownership = doGetLockOwnershipState().join();
        if (ownership.isLockedBy(sessionId, threadId)) {
            lockStates.put(threadId, new LockState(sessionId, ownership.getFence(), ownership.getLockCount()));
            return ownership.getFence();
        }

        throw new IllegalMonitorStateException("Current thread is not owner of the Lock[" + proxyName + "]");
    }

    @Override
    public final boolean isLocked() {
        long sessionId = getSession();
        long threadId = getThreadId();
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            validateLocalLockState(sessionId, threadId, lockState);
            return true;
        }

        // If I learn from the response that I am the current lock owner, it means that
        // an earlier lock() request of mine failed with operation timeout on my side
        // but actually committed on the Raft group. In this case, I already acquired
        // the session before I made the failed lock() call, so there is no need to acquire it here.

        RaftLockOwnershipState ownership = doGetLockOwnershipState().join();
        if (ownership.isLockedBy(sessionId, threadId)) {
            lockStates.put(threadId, new LockState(sessionId, ownership.getFence(), ownership.getLockCount()));
            return true;
        }

        return ownership.isLocked();
    }

    @Override
    public final boolean isLockedByCurrentThread() {
        try {
            return getFence() != INVALID_FENCE;
        } catch (IllegalMonitorStateException ignored) {
            return false;
        }
    }

    @Override
    public final int getLockCountIfLockedByCurrentThread() {
        long sessionId = getSession();
        long threadId = getThreadId();
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            validateLocalLockState(sessionId, threadId, lockState);
            return lockState.lockCount;
        } else if (sessionId == NO_SESSION_ID) {
            return 0;
        }

        // If I learn from the response that I am the current lock owner, it means that
        // an earlier lock() request of mine failed with operation timeout on my side
        // but actually committed on the Raft group. In this case, I already acquired
        // the session before I made the failed lock() call, so there is no need to acquire it here.

        RaftLockOwnershipState ownership = doGetLockOwnershipState().join();
        if (ownership.getSessionId() == sessionId && ownership.getThreadId() == threadId) {
            lockStates.put(threadId, new LockState(sessionId, ownership.getFence(), ownership.getLockCount()));
            return ownership.getLockCount();
        }

        return 0;
    }

    private long tryReentrantLock(long threadId) {
        LockState lockState = lockStates.get(threadId);
        if (lockState != null) {
            validateLocalLockState(getSession(), threadId, lockState);
            lockState.lockCount++;
            return lockState.fence;
        }

        return INVALID_FENCE;
    }

    private void validateLocalLockState(long sessionId, long threadId, LockState lockState) {
        if (lockState.sessionId != sessionId) {
            lockStates.remove(threadId);
            throw new IllegalMonitorStateException(
                    "Current thread is not owner of the Lock[" + proxyName + "] because Session[" + lockState.sessionId
                            + "] is closed by server!");
        }
    }

    // !!! ONLY FOR TESTING !!!
    public final long getLocalLockFence() {
        LockState lockState = lockStates.get(getThreadId());
        return lockState != null ? lockState.fence : INVALID_FENCE;
    }

    // !!! ONLY FOR TESTING !!!
    public final long getLocalLockCount() {
        LockState lockState = lockStates.get(getThreadId());
        return lockState != null ? lockState.lockCount : 0;
    }

    @Override
    public void destroy() {
        lockStates.clear();
    }

    @Override
    public final String getName() {
        return proxyName;
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    protected abstract InternalCompletableFuture<RaftLockOwnershipState> doLock(long sessionId, long threadId,
                                                                                UUID invocationUid);

    protected abstract InternalCompletableFuture<RaftLockOwnershipState> doTryLock(long sessionId, long threadId,
                                                                                   UUID invocationUid, long timeoutMillis);

    protected abstract InternalCompletableFuture<Object> doUnlock(long sessionId, long threadId, UUID invocationUid,
                                                                  int releaseCount);

    protected abstract InternalCompletableFuture<Object> doForceUnlock(UUID invocationUid, long expectedFence);

    protected abstract InternalCompletableFuture<RaftLockOwnershipState> doGetLockOwnershipState();

    private static class LockState {
        final long sessionId;
        final long fence;
        int lockCount;

        LockState(long sessionId, long fence, int lockCount) {
            this.sessionId = sessionId;
            this.fence = fence;
            this.lockCount = lockCount;
        }
    }
}
