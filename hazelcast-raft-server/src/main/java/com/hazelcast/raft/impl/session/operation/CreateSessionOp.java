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

package com.hazelcast.raft.impl.session.operation;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;
import com.hazelcast.raft.impl.session.SessionService;
import com.hazelcast.raft.impl.session.RaftSessionServiceDataSerializerHook;

import java.io.IOException;

/**
 * Creates a new session for the given endpoint and returns its id.
 * This operation does not check if the given endpoint has another active session on the Raft group.
 */
public class CreateSessionOp extends RaftOp implements InvocationTargetLeaveAware, IdentifiedDataSerializable {

    // used for diagnostics
    private Address endpoint;

    public CreateSessionOp() {
    }

    public CreateSessionOp(Address endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        SessionService service = getService();
        return service.createNewSession(groupId, endpoint);
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        return true;
    }

    @Override
    public String getServiceName() {
        return SessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSessionServiceDataSerializerHook.CREATE_SESSION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(endpoint);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", endpoint=").append(endpoint);
    }
}
