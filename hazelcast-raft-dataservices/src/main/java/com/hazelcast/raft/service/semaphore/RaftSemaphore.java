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

package com.hazelcast.raft.service.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.Collections.unmodifiableCollection;

/**
 * TODO: Javadoc Pending...
 */
public class RaftSemaphore extends BlockingResource<SemaphoreInvocationKey> implements IdentifiedDataSerializable {

    private boolean initialized;
    private int available;
    private final Map<Long, SessionState> sessionStates = new HashMap<Long, SessionState>();

    RaftSemaphore() {
    }

    RaftSemaphore(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    Collection<SemaphoreInvocationKey> init(int permits) {
        if (initialized || available != 0) {
            throw new IllegalStateException();
        }

        available = permits;
        initialized = true;

        return assignPermitsToWaitKeys();
    }

    int getAvailable() {
        return available;
    }

    boolean isAvailable(int permits) {
        checkPositive(permits, "Permits should be positive!");
        return available >= permits;
    }

    AcquireResult acquire(SemaphoreInvocationKey key, boolean wait) {
        SessionState state = sessionStates.get(key.sessionId());
        if (state != null && state.invocationRefUids.containsKey(Tuple2.of(key.threadId(), key.invocationUid()))) {
            return new AcquireResult(key.permits(), Collections.<SemaphoreInvocationKey>emptyList());
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(key.sessionId(), key.threadId(), key.invocationUid());

        if (!isAvailable(key.permits())) {
            if (wait) {
                waitKeys.add(key);
            }

            return new AcquireResult(0, cancelled);
        }

        assignPermitsToInvocation(key.sessionId(), key.threadId(), key.invocationUid(), key.permits());

        return new AcquireResult(key.permits(), cancelled);
    }

    private void assignPermitsToInvocation(long sessionId, long threadId, UUID invocationUid, int permits) {
        if (sessionId == NO_SESSION_ID) {
            available -= permits;
            return;
        }

        SessionState state = sessionStates.get(sessionId);
        if (state == null) {
            state = new SessionState();
            sessionStates.put(sessionId, state);
        }

        if (state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits) == null) {
            state.acquiredPermits += permits;
            available -= permits;
        }
    }

    ReleaseResult release(long sessionId, long threadId, UUID invocationUid, int permits) {
        checkPositive(permits, "Permits should be positive!");

        if (sessionId != NO_SESSION_ID) {
            SessionState state = sessionStates.get(sessionId);
            if (state == null) {
                return ReleaseResult.failed(cancelWaitKeys(sessionId, threadId, invocationUid));
            }

            if (state.invocationRefUids.containsKey(Tuple2.of(threadId, invocationUid))) {
                return ReleaseResult.successful(Collections.<SemaphoreInvocationKey>emptyList(),
                        Collections.<SemaphoreInvocationKey>emptyList());
            }

            if (state.acquiredPermits < permits) {
                return ReleaseResult.failed(cancelWaitKeys(sessionId, threadId, invocationUid));
            }

            state.acquiredPermits -= permits;
            state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits);
        }

        available += permits;

        // order is important...
        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);
        Collection<SemaphoreInvocationKey> acquired = assignPermitsToWaitKeys();

        return ReleaseResult.successful(acquired, cancelled);
    }

    private Collection<SemaphoreInvocationKey> cancelWaitKeys(long sessionId, long threadId, UUID invocationUid) {
        List<SemaphoreInvocationKey> cancelled = new ArrayList<SemaphoreInvocationKey>(0);
        Iterator<SemaphoreInvocationKey> iter = waitKeys.iterator();
        while (iter.hasNext()) {
            SemaphoreInvocationKey waitKey = iter.next();
            if (waitKey.sessionId() == sessionId && waitKey.threadId() == threadId
                    && !waitKey.invocationUid().equals(invocationUid)) {
                cancelled.add(waitKey);
                iter.remove();
            }
        }

        return cancelled;
    }

    private Collection<SemaphoreInvocationKey> assignPermitsToWaitKeys() {
        List<SemaphoreInvocationKey> assigned = new ArrayList<SemaphoreInvocationKey>();
        Set<UUID> assignedInvocationUids = new HashSet<UUID>();
        Iterator<SemaphoreInvocationKey> iterator = waitKeys.iterator();
        while (iterator.hasNext()) {
            SemaphoreInvocationKey key = iterator.next();
            if (assignedInvocationUids.contains(key.invocationUid())) {
                iterator.remove();
                assigned.add(key);
            } else if (key.permits() <= available) {
                iterator.remove();
                if (assignedInvocationUids.add(key.invocationUid())) {
                    assigned.add(key);
                    assignPermitsToInvocation(key.sessionId(), key.threadId(), key.invocationUid(), key.permits());
                }
            }
        }

        return assigned;
    }

    AcquireResult drain(long sessionId, long threadId, UUID invocationUid) {
        SessionState state = sessionStates.get(sessionId);
        if (state != null) {
            Integer permits = state.invocationRefUids.get(Tuple2.of(threadId, invocationUid));
            if (permits != null) {
                return new AcquireResult(permits, Collections.<SemaphoreInvocationKey>emptyList());
            }
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);

        int drained = available;
        if (drained > 0) {
            assignPermitsToInvocation(sessionId, threadId, invocationUid, drained);
        }
        available = 0;

        return new AcquireResult(drained, cancelled);
    }

    ReleaseResult change(long sessionId, long threadId, UUID invocationUid, int permits) {
        if (permits == 0) {
            return ReleaseResult.failed(Collections.<SemaphoreInvocationKey>emptyList());
        }

        Collection<SemaphoreInvocationKey> cancelled = cancelWaitKeys(sessionId, threadId, invocationUid);

        if (sessionId != NO_SESSION_ID) {
            SessionState state = sessionStates.get(sessionId);
            if (state == null) {
                state = new SessionState();
                sessionStates.put(sessionId, state);
            }

            if (state.invocationRefUids.containsKey(Tuple2.of(threadId, invocationUid))) {
                Collection<SemaphoreInvocationKey> c = Collections.emptyList();
                return ReleaseResult.successful(c, c);
            }

            state.invocationRefUids.put(Tuple2.of(threadId, invocationUid), permits);
        }

        available += permits;
        initialized = true;

        Collection<SemaphoreInvocationKey> acquired =
                permits > 0 ? assignPermitsToWaitKeys() : Collections.<SemaphoreInvocationKey>emptyList();

        return ReleaseResult.successful(acquired, cancelled);
    }

    @Override
    protected void onInvalidateSession(long sessionId, Long2ObjectHashMap<Object> responses) {
        SessionState state = sessionStates.get(sessionId);
        if (state != null && state.acquiredPermits > 0) {
            // remove the session after release() because release() checks existence of the session
            ReleaseResult result = release(sessionId, 0, newUnsecureUUID(), state.acquiredPermits);
            sessionStates.remove(sessionId);
            assert result.cancelled.isEmpty();
            for (SemaphoreInvocationKey key : result.acquired) {
                responses.put(key.commitIndex(), Boolean.TRUE);
            }
        }
    }

    @Override
    protected Collection<Long> getOwnerSessions() {
        Set<Long> activeSessionIds = new HashSet<Long>();
        for (Map.Entry<Long, SessionState> e : sessionStates.entrySet()) {
            if (e.getValue().acquiredPermits > 0) {
                activeSessionIds.add(e.getKey());
            }
        }

        return activeSessionIds;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.RAFT_SEMAPHORE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeBoolean(initialized);
        out.writeInt(available);
        out.writeInt(sessionStates.size());
        for (Entry<Long, SessionState> e1 : sessionStates.entrySet()) {
            out.writeLong(e1.getKey());
            SessionState state = e1.getValue();
            out.writeInt(state.invocationRefUids.size());
            for (Entry<Tuple2<Long, UUID>, Integer> e2 : state.invocationRefUids.entrySet()) {
                Tuple2<Long, UUID> t = e2.getKey();
                UUID invocationUid = t.element2;
                int permits = e2.getValue();
                out.writeLong(t.element1);
                out.writeLong(invocationUid.getLeastSignificantBits());
                out.writeLong(invocationUid.getMostSignificantBits());
                out.writeInt(permits);
            }
            out.writeInt(state.acquiredPermits);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        initialized = in.readBoolean();
        available = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            SessionState state = new SessionState();
            int refUidCount = in.readInt();
            for (int j = 0; j < refUidCount; j++) {
                long threadId = in.readLong();
                long least = in.readLong();
                long most = in.readLong();
                int permits = in.readInt();
                state.invocationRefUids.put(Tuple2.of(threadId, new UUID(most, least)), permits);
            }

            state.acquiredPermits = in.readInt();

            sessionStates.put(sessionId, state);
        }
    }

    @Override
    public String toString() {
        return "RaftSemaphore{" + "groupId=" + groupId + ", name='" + name + '\'' + ", initialized=" + initialized
                + ", available=" + available + ", sessionStates=" + sessionStates + ", waitKeys=" + waitKeys + '}';
    }

    static class AcquireResult {

        final int acquired;

        final Collection<SemaphoreInvocationKey> cancelled; // can be populated when both acquired and not acquired

        private AcquireResult(int acquired, Collection<SemaphoreInvocationKey> cancelled) {
            this.acquired = acquired;
            this.cancelled = unmodifiableCollection(cancelled);
        }
    }

    static class ReleaseResult {

        private static ReleaseResult failed(Collection<SemaphoreInvocationKey> cancelled) {
            return new ReleaseResult(false, Collections.<SemaphoreInvocationKey>emptyList(), cancelled);
        }

        private static ReleaseResult successful(Collection<SemaphoreInvocationKey> acquired, Collection<SemaphoreInvocationKey> cancelled) {
            return new ReleaseResult(true, acquired, cancelled);
        }

        final boolean success;
        final Collection<SemaphoreInvocationKey> acquired;
        final Collection<SemaphoreInvocationKey> cancelled;

        private ReleaseResult(boolean success,
                              Collection<SemaphoreInvocationKey> acquired, Collection<SemaphoreInvocationKey> cancelled) {
            this.success = success;
            this.acquired = unmodifiableCollection(acquired);
            this.cancelled = unmodifiableCollection(cancelled);
        }
    }

    private static class SessionState {
        private final Map<Tuple2<Long, UUID>, Integer> invocationRefUids = new HashMap<Tuple2<Long, UUID>, Integer>();
        private int acquiredPermits;

        @Override
        public String toString() {
            return "SessionState{" + "invocationRefUids=" + invocationRefUids + ", acquiredPermits=" + acquiredPermits + '}';
        }
    }

}