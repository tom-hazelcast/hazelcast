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

package com.hazelcast.raft.impl.service.operation.integration;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class AppendFailureResponseOp extends AsyncRaftOp {

    private AppendFailureResponse appendResponse;

    public AppendFailureResponseOp() {
    }

    public AppendFailureResponseOp(RaftGroupId groupId, AppendFailureResponse appendResponse) {
        super(groupId);
        this.appendResponse = appendResponse;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handleAppendResponse(groupId, appendResponse);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(appendResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        appendResponse = in.readObject();
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.APPEND_FAILURE_RESPONSE_OP;
    }

}