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

import com.hazelcast.config.raft.RaftGroupConfig;
import com.hazelcast.config.raft.RaftLockConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftNode;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.session.SessionAwareService;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.lock.operation.InvalidateWaitEntriesOp;
import com.hazelcast.raft.service.lock.proxy.RaftLockProxy;
import com.hazelcast.raft.service.session.SessionManagerService;
import com.hazelcast.raft.impl.RaftGroupLifecycleAwareService;
import com.hazelcast.raft.service.spi.RaftRemoteService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockService implements ManagedService, SnapshotAwareService<LockRegistrySnapshot>,
        RaftRemoteService, RaftGroupLifecycleAwareService, SessionAwareService {

    private static final long TRY_LOCK_TIMEOUT_TASK_PERIOD_MILLIS = 500;
    static final long TRY_LOCK_TIMEOUT_TASK_UPPER_BOUND_MILLIS = 1500;
    public static final String SERVICE_NAME = "hz:raft:lockService";

    private final ConcurrentMap<RaftGroupId, LockRegistry> registries = new ConcurrentHashMap<RaftGroupId, LockRegistry>();
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile RaftService raftService;
    private volatile SessionAccessor sessionAccessor;

    public RaftLockService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(new InvalidateExpiredWaitEntriesPeriodicTask(),
                TRY_LOCK_TIMEOUT_TASK_PERIOD_MILLIS, TRY_LOCK_TIMEOUT_TASK_PERIOD_MILLIS, MILLISECONDS);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        registries.clear();
    }

    @Override
    public LockRegistrySnapshot takeSnapshot(RaftGroupId groupId, long commitIndex) {
        LockRegistry registry = registries.get(groupId);
        return registry != null ? registry.toSnapshot() : null;
    }

    @Override
    public void restoreSnapshot(RaftGroupId groupId, long commitIndex, LockRegistrySnapshot snapshot) {
        if (snapshot != null) {
            LockRegistry registry = getLockRegistry(groupId);
            Map<LockInvocationKey, Long> timeouts = registry.restore(snapshot);
            ExecutionService executionService = nodeEngine.getExecutionService();
            for (Entry<LockInvocationKey, Long> e : timeouts.entrySet()) {
                LockInvocationKey key = e.getKey();
                long waitTimeMs = e.getValue();
                if (waitTimeMs < TRY_LOCK_TIMEOUT_TASK_UPPER_BOUND_MILLIS) {
                    Runnable task = new InvalidateExpiredWaitEntriesTask(groupId, key);
                    executionService.schedule(task, waitTimeMs, MILLISECONDS);
                }
            }
        }
    }

    @Override
    public void setSessionAccessor(SessionAccessor accessor) {
        this.sessionAccessor = accessor;
    }

    @Override
    public void onSessionInvalidated(RaftGroupId groupId, long sessionId) {
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            logger.fine("No lock registry for " + groupId + " and session: " + sessionId);
            return;
        }

        Tuple2<Collection<Long>, Collection<Long>> t = registry.invalidateSession(sessionId);
        if (t != null) {
            Collection<Long> invalidations = t.element1;
            Collection<Long> acquires = t.element2;
            completeFutures(groupId, invalidations, new SessionExpiredException(sessionId));
            completeFutures(groupId, acquires, true);
        }
    }

    @Override
    public ILock createRaftObjectProxy(String name) {
        try {
            RaftGroupId groupId = createRaftGroup(name).get();
            SessionManagerService sessionManager = nodeEngine.getService(SessionManagerService.SERVICE_NAME);
            return new RaftLockProxy(name, groupId, sessionManager, raftService.getInvocationManager());
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean destroyRaftObject(RaftGroupId groupId, String name) {
        LockRegistry registry = getLockRegistry(groupId);
        Collection<Long> indices = registry.destroyLock(name);
        if (indices == null) {
            return false;
        }
        completeFutures(groupId, indices, new DistributedObjectDestroyedException("Lock[" + name + " is destroyed"));
        return true;
    }

    @Override
    public void onGroupDestroy(final RaftGroupId groupId) {
        LockRegistry registry = registries.get(groupId);
        if (registry != null) {
            Collection<Long> indices = registry.destroy();
            completeFutures(groupId, indices, new DistributedObjectDestroyedException("Lock is destroyed"));
        }
    }

    public ICompletableFuture<RaftGroupId> createRaftGroup(String name) {
        String raftGroupRef = getRaftGroupRef(name);

        RaftInvocationManager invocationManager = raftService.getInvocationManager();
        return invocationManager.createRaftGroup(raftGroupRef);
    }

    private String getRaftGroupRef(String name) {
        RaftLockConfig config = getConfig(name);
        return config != null ? config.getRaftGroupRef() : RaftGroupConfig.DEFAULT_GROUP;
    }

    private RaftLockConfig getConfig(String name) {
        return nodeEngine.getConfig().findRaftLockConfig(name);
    }

    private LockRegistry getLockRegistry(RaftGroupId groupId) {
        checkNotNull(groupId);
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            registry = new LockRegistry(groupId);
            registries.put(groupId, registry);
        }
        return registry;
    }

    public boolean acquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId())) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId());
            boolean acquired = getLockRegistry(groupId).acquire(name, endpoint, commitIndex, invocationUid);
            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                        + invocationUid + ">");
            }
            return acquired;
        }
        throw new SessionExpiredException(endpoint.sessionId());
    }

    public Object tryAcquire(RaftGroupId groupId, String name, LockEndpoint endpoint, long commitIndex, UUID invocationUid,
                          long timeoutMs) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId())) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId());
            boolean acquired = getLockRegistry(groupId).tryAcquire(name, endpoint, commitIndex, invocationUid, timeoutMs);
            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " acquired: " + acquired + " by <" + endpoint + ", "
                        + invocationUid + ">");
            }

            if (timeoutMs > 0 && !acquired) {
                if (timeoutMs <= TRY_LOCK_TIMEOUT_TASK_UPPER_BOUND_MILLIS) {
                    LockInvocationKey key = new LockInvocationKey(name, endpoint, commitIndex, invocationUid);
                    Runnable task = new InvalidateExpiredWaitEntriesTask(groupId, key);
                    ExecutionService executionService = nodeEngine.getExecutionService();
                    executionService.schedule(task, timeoutMs, MILLISECONDS);
                }

                return PostponedResponse.INSTANCE;
            }

            return acquired;
        }
        throw new SessionExpiredException(endpoint.sessionId());
    }

    public void release(RaftGroupId groupId, String name, LockEndpoint endpoint, UUID invocationUid) {
        if (sessionAccessor.isValid(groupId, endpoint.sessionId())) {
            sessionAccessor.heartbeat(groupId, endpoint.sessionId());

            LockRegistry registry = registries.get(groupId);
            if (registry == null) {
                logger.severe("No LockRegistry is found for " + groupId + " to release lock: " + name + " by <" + endpoint
                        + ", " + invocationUid + ">");
                return;
            }

            Collection<LockInvocationKey> waitEntries = registry.release(name, endpoint, invocationUid);

            if (logger.isFineEnabled()) {
                logger.fine("Lock: " + name + " in " + groupId + " is released by " + endpoint);
                if (waitEntries.size() > 0) {
                    LockInvocationKey newOwner = waitEntries.iterator().next();
                    logger.fine("Lock: " + name + " in " + groupId + " is acquired by " + newOwner.endpoint());
                }
            }

            List<Long> indices = new ArrayList<Long>(waitEntries.size());
            for (LockInvocationKey waitEntry : waitEntries) {
                indices.add(waitEntry.commitIndex());
            }
            completeFutures(groupId, indices, true);
        } else {
            throw new IllegalMonitorStateException();
        }
    }

    public void invalidateWaitEntries(RaftGroupId groupId, Collection<LockInvocationKey> keys) {
        // no need to validate the session. if the session is invalid, the corresponding wait entry is gone already
        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            logger.severe("No LockRegistry is found for " + groupId + " to invalidate wait entries: " + keys);
            return;
        }

        List<Long> invalidated = new ArrayList<Long>();
        for (LockInvocationKey key : keys) {
            if (registry.invalidateWaitEntry(key)) {
                invalidated.add(key.commitIndex());
                if (logger.isFineEnabled()) {
                    logger.fine("Wait entry of " + key + " is invalidated.");
                }
            }
        }

        completeFutures(groupId, invalidated, false);
    }

    private void completeFutures(RaftGroupId groupId, Collection<Long> indices, Object result) {
        if (!indices.isEmpty()) {
            RaftNodeImpl raftNode = (RaftNodeImpl) raftService.getRaftNode(groupId);
            for (Long index : indices) {
                raftNode.completeFuture(index, result);
            }
        }
    }

    // queried locally
    private Map<RaftGroupId, Collection<LockInvocationKey>> getExpiredWaitEntries() {
        Map<RaftGroupId, Collection<LockInvocationKey>> timeouts = new HashMap<RaftGroupId, Collection<LockInvocationKey>>();
        long now = Clock.currentTimeMillis();
        for (LockRegistry registry : registries.values()) {
            Collection<LockInvocationKey> t = registry.getExpiredWaitEntries(now);
            if (t.size() > 0) {
                timeouts.put(registry.groupId(), t);
            }
        }
        return timeouts;
    }

    public Tuple2<LockEndpoint, Integer> lockCount(RaftGroupId groupId, String name) {
        checkNotNull(groupId);
        checkNotNull(name);

        LockRegistry registry = registries.get(groupId);
        if (registry == null) {
            return Tuple2.of(null, 0);
        }

        return registry.lockCount(name);
    }

    // queried locally in tests
    LockRegistry getLockRegistryOrNull(RaftGroupId groupId) {
        return registries.get(groupId);
    }

    private class InvalidateExpiredWaitEntriesTask implements Runnable {
        final RaftGroupId groupId;
        final Collection<LockInvocationKey> keys;

        InvalidateExpiredWaitEntriesTask(RaftGroupId groupId, LockInvocationKey key) {
            this.groupId = groupId;
            this.keys = Collections.singleton(key);
        }

        InvalidateExpiredWaitEntriesTask(RaftGroupId groupId, Collection<LockInvocationKey> keys) {
            this.groupId = groupId;
            this.keys = keys;
        }

        @Override
        public void run() {
            try {
                RaftNode raftNode = raftService.getRaftNode(groupId);
                if (raftNode != null) {
                    Future f = raftNode.replicate(new InvalidateWaitEntriesOp(keys));
                    f.get();
                }
            } catch (Exception e) {
                if (logger.isFineEnabled()) {
                    logger.fine("Could not invalidate wait entries: " + keys + " of " + groupId, e);
                }
            }
        }
    }

    private class InvalidateExpiredWaitEntriesPeriodicTask implements Runnable {
        @Override
        public void run() {
            for (Entry<RaftGroupId, Collection<LockInvocationKey>> e : getExpiredWaitEntries().entrySet()) {
                new InvalidateExpiredWaitEntriesTask(e.getKey(), e.getValue()).run();
            }
        }
    }
}
