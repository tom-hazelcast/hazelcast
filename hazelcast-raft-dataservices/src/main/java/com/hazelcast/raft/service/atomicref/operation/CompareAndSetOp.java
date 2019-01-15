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

package com.hazelcast.raft.service.atomicref.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomicref.AtomicReferenceDataSerializerHook;
import com.hazelcast.raft.service.atomicref.RaftAtomicRef;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class CompareAndSetOp extends AbstractAtomicRefOp implements IdentifiedDataSerializable {

    private Data expectedValue;
    private Data newValue;

    public CompareAndSetOp() {
    }

    public CompareAndSetOp(String name, Data expectedValue, Data newValue) {
        super(name);
        this.expectedValue = expectedValue;
        this.newValue = newValue;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicRef ref = getAtomicRef(groupId);
        boolean contains = ref.contains(expectedValue);
        if (contains) {
            ref.set(newValue);
        }
        return contains;
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.COMPARE_AND_SET_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeData(expectedValue);
        out.writeData(newValue);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        expectedValue = in.readData();
        newValue = in.readData();
    }
}
