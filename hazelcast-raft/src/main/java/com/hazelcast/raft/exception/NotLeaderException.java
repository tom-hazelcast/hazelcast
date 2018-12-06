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

package com.hazelcast.raft.exception;

import com.hazelcast.cp.RaftGroupId;
import com.hazelcast.cp.RaftMember;

/**
 * A {@code RaftException} which is thrown when a leader-only request is received by a non-leader member.
 */
public class NotLeaderException extends RaftException {
    public NotLeaderException(RaftGroupId groupId, RaftMember local, RaftMember leader) {
        super(local + " is not LEADER of " + groupId + ". Known leader is: "
                + (leader != null ? leader : "N/A") , leader);
    }
}
