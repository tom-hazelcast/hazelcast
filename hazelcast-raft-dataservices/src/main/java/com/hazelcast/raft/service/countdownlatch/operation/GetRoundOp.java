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

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.proxy.InvocationTargetLeaveAware;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchDataSerializerHook;
import com.hazelcast.raft.service.countdownlatch.RaftCountDownLatchService;

/**
 * TODO: Javadoc Pending...
 */
public class GetRoundOp extends AbstractCountDownLatchOp implements InvocationTargetLeaveAware {

    public GetRoundOp() {
    }

    public GetRoundOp(String name) {
        super(name);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftCountDownLatchService service = getService();
        return service.getRound(groupId, name);
    }

    @Override
    public boolean isSafeToRetryOnTargetLeave() {
        return true;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.GET_ROUND_OP;
    }
}
