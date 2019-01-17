/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.session.SessionExpiredException;
import com.hazelcast.nio.Bits;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.Clock;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.RaftGroupId.dataSize;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.ACQUIRE_PERMITS_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.AVAILABLE_PERMITS_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.CHANGE_PERMITS_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.DRAIN_PERMITS_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.INIT_SEMAPHORE_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.client.SemaphoreMessageTaskFactoryProvider.RELEASE_PERMITS_TYPE;
import static com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy.DRAIN_SESSION_ACQ_COUNT;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.ThreadUtil.getThreadId;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.lang.Math.max;

/**
 * Client-side session-aware proxy of Raft-based {@link ISemaphore}
 */
class RaftSessionAwareSemaphoreProxy extends ClientProxy implements ISemaphore {

    private static final ClientMessageDecoder INT_RESPONSE_DECODER = new IntResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();


    private final ClientProxySessionManager sessionManager;
    private final RaftGroupId groupId;
    private final String objectName;

    RaftSessionAwareSemaphoreProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(RaftSemaphoreService.SERVICE_NAME, proxyName, context);
        this.sessionManager = getClient().getProxySessionManager();
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean init(int permits) {
        checkNotNegative(permits, "Permits must be non-negative!");

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, NO_SESSION_ID, dataSize, INIT_SEMAPHORE_TYPE);
        msg.set(permits);
        msg.updateFrameLength();

        InternalCompletableFuture<Boolean> future = invoke(msg, BOOLEAN_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public void acquire() {
        acquire(1);
    }

    @Override
    public void acquire(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES * 5
                + Bits.INT_SIZE_IN_BYTES;
        for (;;) {
            long sessionId = sessionManager.acquireSession(groupId, permits);
            ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, ACQUIRE_PERMITS_TYPE);
            msg.set(threadId);
            msg.set(invocationUid.getLeastSignificantBits());
            msg.set(invocationUid.getMostSignificantBits());
            msg.set(permits);
            msg.set(-1L);
            msg.updateFrameLength();

            try {
                invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
                return;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(groupId, sessionId);
            }
        }
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    @Override
    public boolean tryAcquire(int permits) {
        return tryAcquire(permits, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1, timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        checkPositive(permits, "Permits must be positive!");
        long timeoutMs = max(0, unit.toMillis(timeout));
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES * 5
                + Bits.INT_SIZE_IN_BYTES;
        long start;
        for (;;) {
            start = Clock.currentTimeMillis();
            long sessionId = sessionManager.acquireSession(groupId, permits);
            ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, ACQUIRE_PERMITS_TYPE);
            msg.set(threadId);
            msg.set(invocationUid.getLeastSignificantBits());
            msg.set(invocationUid.getMostSignificantBits());
            msg.set(permits);
            msg.set(timeoutMs);
            msg.updateFrameLength();

            try {
                InternalCompletableFuture<Boolean> future = invoke(msg, BOOLEAN_RESPONSE_DECODER);
                boolean acquired = future.join();
                if (!acquired) {
                    sessionManager.releaseSession(groupId, sessionId, permits);
                }
                return acquired;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(groupId, sessionId);
                timeoutMs -= (Clock.currentTimeMillis() - start);
                if (timeoutMs <= 0) {
                    return false;
                }
            }
        }
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        checkPositive(permits, "Permits must be positive!");
        long sessionId = sessionManager.getSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, RELEASE_PERMITS_TYPE);
        msg.set(threadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(permits);
        msg.updateFrameLength();
        try {
            invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(groupId, sessionId);
            throw e;
        } finally {
            sessionManager.releaseSession(groupId, sessionId, permits);
        }
    }

    @Override
    public int availablePermits() {
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, NO_SESSION_ID, dataSize, AVAILABLE_PERMITS_TYPE);
        msg.updateFrameLength();

        InternalCompletableFuture<Integer> future = invoke(msg, INT_RESPONSE_DECODER);
        return future.join();
    }

    @Override
    public int drainPermits() {
        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();
        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName)
                + Bits.LONG_SIZE_IN_BYTES * 4;
        for (;;) {
            long sessionId = sessionManager.acquireSession(groupId, DRAIN_SESSION_ACQ_COUNT);
            ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, DRAIN_PERMITS_TYPE);
            msg.set(threadId);
            msg.set(invocationUid.getLeastSignificantBits());
            msg.set(invocationUid.getMostSignificantBits());
            msg.updateFrameLength();

            try {
                InternalCompletableFuture<Integer> future = invoke(msg, INT_RESPONSE_DECODER);
                int count = future.join();
                sessionManager.releaseSession(groupId, sessionId, DRAIN_SESSION_ACQ_COUNT - count);
                return count;
            } catch (SessionExpiredException e) {
                sessionManager.invalidateSession(groupId, sessionId);
            }
        }
    }

    @Override
    public void reducePermits(int reduction) {
        checkNotNegative(reduction, "Reduction must be non-negative!");
        if (reduction == 0) {
            return;
        }

        long sessionId = sessionManager.acquireSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, CHANGE_PERMITS_TYPE);
        msg.set(threadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(-reduction);
        msg.updateFrameLength();

        try {
            invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(groupId, sessionId);
            throw e;
        } finally {
            sessionManager.releaseSession(groupId, sessionId);
        }
    }

    @Override
    public void increasePermits(int increase) {
        checkNotNegative(increase, "Increase must be non-negative!");
        if (increase == 0) {
            return;
        }

        long sessionId = sessionManager.acquireSession(groupId);
        if (sessionId == NO_SESSION_ID) {
            throw new IllegalStateException("No valid session!");
        }

        long threadId = getThreadId();
        UUID invocationUid = newUnsecureUUID();

        int dataSize = ClientMessage.HEADER_SIZE + dataSize(groupId) + calculateDataSize(objectName) + Bits.LONG_SIZE_IN_BYTES * 4
                + Bits.INT_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, sessionId, dataSize, CHANGE_PERMITS_TYPE);
        msg.set(threadId);
        msg.set(invocationUid.getLeastSignificantBits());
        msg.set(invocationUid.getMostSignificantBits());
        msg.set(increase);
        msg.updateFrameLength();

        try {
            invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
        } catch (SessionExpiredException e) {
            sessionManager.invalidateSession(groupId, sessionId);
            throw e;
        } finally {
            sessionManager.releaseSession(groupId, sessionId);
        }
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(DESTROY_TYPE);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(objectName);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    static ClientMessage prepareClientMessage(RaftGroupId groupId, String name, long sessionId, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(name);
        msg.set(sessionId);
        return msg;
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage msg, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), msg, name).invoke();
        return new ClientDelegatingFuture<T>(future, getContext().getSerializationService(), decoder);
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

}