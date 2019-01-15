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

package com.hazelcast.raft.service.countdownlatch.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.InvocationTargetLeaveAware;
import com.hazelcast.raft.impl.util.PostponedResponse;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Operation for {@link com.hazelcast.core.ICountDownLatch#await(long, TimeUnit)}
 */
public class AwaitOp extends AbstractCountDownLatchOp implements InvocationTargetLeaveAware {

    private long timeoutMillis;

    public AwaitOp() {
    }

    public AwaitOp(String name, long timeoutMillis) {
        super(name);
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftCountDownLatchService service = getService();
        if (service.await(groupId, name, commitIndex, timeoutMillis)) {
            return true;
        }

        return timeoutMillis > 0 ? PostponedResponse.INSTANCE : false;
    }

    @Override
    public boolean isRetryableOnTargetLeave() {
        return true;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.AWAIT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeLong(timeoutMillis);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        timeoutMillis = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", timeoutMillis=").append(timeoutMillis);
    }
}
