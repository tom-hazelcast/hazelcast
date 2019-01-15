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

package com.hazelcast.client.cp.internal.datastructures.atomicref;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.APPLY_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.COMPARE_AND_SET_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.CONTAINS_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.DESTROY_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.GET_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.client.AtomicRefMessageTaskFactoryProvider.SET_TYPE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.NO_RETURN_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_OLD_VALUE;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * TODO: Javadoc Pending...
 */
class RaftAtomicRefProxy<T> extends ClientProxy implements IAtomicReference<T> {

    private static final ClientMessageDecoder DATA_RESPONSE_DECODER = new DataResponseDecoder();
    private static final ClientMessageDecoder BOOLEAN_RESPONSE_DECODER = new BooleanResponseDecoder();


    private final CPGroupId groupId;
    private final String objectName;

    RaftAtomicRefProxy(ClientContext context, CPGroupId groupId, String proxyName, String objectName) {
        super(RaftAtomicRefService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public T get() {
        return getAsync().join();
    }

    @Override
    public void set(T newValue) {
        setAsync(newValue).join();
    }

    @Override
    public T getAndSet(T newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public T setAndGet(T update) {
        setAsync(update).join();
        return update;
    }

    @Override
    public boolean isNull() {
        return isNullAsync().join();
    }

    @Override
    public void clear() {
        clearAsync().join();
    }

    @Override
    public boolean contains(T value) {
        return containsAsync(value).join();
    }

    @Override
    public void alter(IFunction<T, T> function) {
        alterAsync(function).join();
    }

    @Override
    public T alterAndGet(IFunction<T, T> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public T getAndAlter(IFunction<T, T> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public <R> R apply(IFunction<T, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(T expect, T update) {
        Data expectedData = getContext().getSerializationService().toData(expect);
        Data newData = getContext().getSerializationService().toData(update);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName)
                + nullableSize(expectedData) + nullableSize(newData);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, COMPARE_AND_SET_TYPE);
        writeNullableData(msg, expectedData);
        writeNullableData(msg, newData);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<T> getAsync() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, GET_TYPE);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(T newValue) {
        Data data = getContext().getSerializationService().toData(newValue);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, SET_TYPE);
        writeNullableData(msg, data);
        msg.set(false);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<T> getAndSetAsync(T newValue) {
        Data data = getContext().getSerializationService().toData(newValue);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, SET_TYPE);
        writeNullableData(msg, data);
        msg.set(true);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Boolean> isNullAsync() {
        return containsAsync(null);
    }

    @Override
    public InternalCompletableFuture<Void> clearAsync() {
        return setAsync(null);
    }

    @Override
    public InternalCompletableFuture<Boolean> containsAsync(T expected) {
        Data data = getContext().getSerializationService().toData(expected);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName)
                + nullableSize(data) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, CONTAINS_TYPE);
        writeNullableData(msg, data);
        msg.set(false);
        msg.updateFrameLength();

        return invoke(msg, BOOLEAN_RESPONSE_DECODER);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        return invokeApply(function, NO_RETURN_VALUE, true);
    }

    @Override
    public InternalCompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_NEW_VALUE, true);
    }

    @Override
    public InternalCompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_OLD_VALUE, true);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<T, R> function) {
        return invokeApply(function, RETURN_NEW_VALUE, false);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName);
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, DESTROY_TYPE);
        msg.updateFrameLength();

        invoke(msg, BOOLEAN_RESPONSE_DECODER).join();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    private int nullableSize(Data data) {
        return Bits.BOOLEAN_SIZE_IN_BYTES + (data != null ? (Bits.INT_SIZE_IN_BYTES + data.totalSize()) : 0);
    }

    private void writeNullableData(ClientMessage msg, Data data) {
        boolean exists = (data != null);
        msg.set(exists);
        if (exists) {
            msg.set(data);
        }
    }

    private <T2, T3> InternalCompletableFuture<T3> invokeApply(IFunction<T, T2> function, ReturnValueType returnValueType, boolean alter) {
        checkTrue(function != null, "Function cannot be null");
        Data data = getContext().getSerializationService().toData(function);
        int dataSize = ClientMessage.HEADER_SIZE + RaftGroupId.dataSize(groupId) + calculateDataSize(objectName)
                + nullableSize(data) + calculateDataSize(returnValueType.name()) + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage msg = prepareClientMessage(groupId, objectName, dataSize, APPLY_TYPE);
        writeNullableData(msg, data);
        msg.set(returnValueType.name());
        msg.set(alter);
        msg.updateFrameLength();

        return invoke(msg, DATA_RESPONSE_DECODER);
    }

    private <T> InternalCompletableFuture<T> invoke(ClientMessage clientMessage, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, name).invoke();
        return new ClientDelegatingFuture<T>(future, getContext().getSerializationService(), decoder);
    }

    private static ClientMessage prepareClientMessage(CPGroupId groupId, String name, int dataSize, int messageTypeId) {
        ClientMessage msg = ClientMessage.createForEncode(dataSize);
        msg.setMessageType(messageTypeId);
        msg.setRetryable(false);
        msg.setOperationName("");
        RaftGroupId.writeTo(groupId, msg);
        msg.set(name);
        return msg;
    }

    private static class DataResponseDecoder implements ClientMessageDecoder {
        @Override
        public Data decodeClientMessage(ClientMessage msg) {
            boolean exists = msg.getBoolean();
            return exists ? msg.getData() : null;
        }
    }

    private static class BooleanResponseDecoder implements ClientMessageDecoder {
        @Override
        public Boolean decodeClientMessage(ClientMessage msg) {
            return msg.getBoolean();
        }
    }

}
