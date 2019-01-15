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

package com.hazelcast.raft.service.lock.client;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.nio.Bits;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.session.SessionExpiredException;
import com.hazelcast.raft.service.lock.RaftLockService;
import com.hazelcast.raft.service.session.SessionAwareProxy;
import com.hazelcast.raft.service.session.SessionManagerProvider;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.UuidUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.raft.impl.RaftGroupIdImpl.dataSize;
import static com.hazelcast.raft.service.lock.RaftLockService.INVALID_FENCE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.CREATE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.FORCE_UNLOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_COUNT_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_FENCE_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.LOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.TRY_LOCK_TYPE;
import static com.hazelcast.raft.service.lock.client.LockMessageTaskFactoryProvider.UNLOCK_TYPE;
import static com.hazelcast.raft.service.session.AbstractSessionManager.NO_SESSION_ID;
import static com.hazelcast.raft.service.util.ClientAccessor.getClient;
import static com.hazelcast.util.ThreadUtil.getThreadId;

/**
 * TODO: Javadoc Pending...
 */
public class RaftLockProxy extends SessionAwareProxy implements ILock {

    static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();
    static final ClientMessageDecoder LONG_RESPONSE_DECODER = new LongResponseDecoder();

    public static ILock create(HazelcastInstance instance, String name) {
        int dataSize = ClientMessage.HEADER_SIZE + calculateDataSize(name);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(CREATE_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        msg.set(name);
        msg.updateFrameLength();

        HazelcastClientInstanceImpl client = getClient(instance);
        ClientInvocationFuture f = new ClientInvocation(client, msg, name).invoke();

        InternalCompletableFuture<RaftGroupId> future = new ClientDelegatingFuture<RaftGroupId>(f, client.getSerializationService(),
                new ClientMessageDecoder() {
            @Override
            public RaftGroupId decodeClientMessage(ClientMessage msg) {
                return RaftGroupIdImpl.readFrom(msg);
            }
        });

        RaftGroupId groupId = future.join();
        return new RaftLockProxy(instance, groupId, name);
    }

    private final HazelcastClientInstanceImpl client;
    private final RaftGroupId groupId;
    private final String name;

    private RaftLockProxy(HazelcastInstance instance, RaftGroupId groupId, String name) {
        super(SessionManagerProvider.get(getClient(instance)), groupId);
        this.client = getClient(instance);
        this.groupId = groupId;
        this.name = name;
    }

    @Override
    public void lock() {
        UUID invUid = UuidUtil.newUnsecureUUID();
        for (;;) {
            long sessionId = acquireSession();
            ClientMessage msg = encodeRequest(LOCK_TYPE, groupId, name, sessionId, getThreadId(), invUid);
            try {
                invoke(client, name, msg, LONG_RESPONSE_DECODER).join();
                break;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public boolean tryLock() {
        return tryLock(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        UUID invUid = UuidUtil.newUnsecureUUID();
        long timeoutMs = Math.max(0, unit.toMillis(time));
        for (;;) {
            long sessionId = acquireSession();
            ClientMessage msg = encodeRequest(TRY_LOCK_TYPE, groupId, name, sessionId, getThreadId(), invUid, timeoutMs);
            try {
                InternalCompletableFuture<Long> future = invoke(client, name, msg, LONG_RESPONSE_DECODER);
                boolean locked = (future.join() != INVALID_FENCE);
                if (!locked) {
                    releaseSession(sessionId);
                }
                return locked;
            } catch (SessionExpiredException e) {
                invalidateSession(sessionId);
            }
        }
    }

    @Override
    public void unlock() {
        long sessionId = getSession();
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalMonitorStateException();
        }
        UUID invUid = UuidUtil.newUnsecureUUID();
        ClientMessage msg = encodeRequest(UNLOCK_TYPE, groupId, name, sessionId, getThreadId(), invUid);
        try {
            invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER).join();
        } catch (SessionExpiredException e) {
            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
        } finally {
            releaseSession(sessionId);
        }
    }

    @Override
    public boolean isLocked() {
        return getLockCount() > 0;
    }

    @Override
    public boolean isLockedByCurrentThread() {
        long sessionId = getSession();
        if (sessionId == NO_SESSION_ID) {
            return false;
        }
        ClientMessage msg = encodeRequest(LOCK_COUNT_TYPE, groupId, name, sessionId, getThreadId());
        InternalCompletableFuture<Integer> future = invoke(client, name, msg, INT_RESPONSE_DECODER);
        return future.join() > 0;
    }

    @Override
    public int getLockCount() {
        ClientMessage msg = encodeRequest(LOCK_COUNT_TYPE, groupId, name, NO_SESSION_ID, 0);
        InternalCompletableFuture<Integer> future = invoke(client, name, msg, INT_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lock(long leaseTime, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        ClientMessage msg = encodeRequest(LOCK_FENCE_TYPE, groupId, name, -1, -1);
        long fence = RaftLockProxy.<Long>invoke(client, name, msg, LONG_RESPONSE_DECODER).join();

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5;
        msg = prepareClientMessage(groupId, name, dataSize, FORCE_UNLOCK_TYPE);
        setRequestParams(msg, -1, -1, UuidUtil.newUnsecureUUID());
        msg.set(fence);
        msg.updateFrameLength();

        invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ICondition newCondition(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRemainingLeaseTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(name);
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(client, name, msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    static <T> InternalCompletableFuture<T> invoke(HazelcastClientInstanceImpl client, String name, ClientMessage msg,
                                                   ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(client, msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, client.getSerializationService(), decoder);
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
            long threadId, UUID invUid) {
        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 4;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.updateFrameLength();
        return msg;
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId,
            long threadId, UUID invUid, long val) {

        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 5;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        setRequestParams(msg, sessionId, threadId, invUid);
        msg.set(val);
        msg.updateFrameLength();
        return msg;
    }

    private static void setRequestParams(ClientMessage msg, long sessionId, long threadId, UUID invUid) {
        msg.set(sessionId);
        msg.set(threadId);
        msg.set(invUid.getLeastSignificantBits());
        msg.set(invUid.getMostSignificantBits());
    }

    static ClientMessage encodeRequest(int messageTypeId, RaftGroupId groupId, String name, long sessionId, long threadId) {
        int dataSize = ClientMessage.HEADER_SIZE
                + dataSize(groupId) + calculateDataSize(name) + Bits.LONG_SIZE_IN_BYTES * 2;
        ClientMessage msg = prepareClientMessage(groupId, name, dataSize, messageTypeId);
        msg.set(sessionId);
        msg.set(threadId);
        msg.updateFrameLength();
        return msg;
    }

    static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupIdImpl.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    private static class IntResponseDecoder implements ClientMessageDecoder {
        @Override
        public Integer decodeClientMessage(ClientMessage msg) {
            return msg.getInt();
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

    private static class LongResponseDecoder implements ClientMessageDecoder {
        @Override
        public Long decodeClientMessage(ClientMessage msg) {
            return msg.getLong();
        }
    }
}
