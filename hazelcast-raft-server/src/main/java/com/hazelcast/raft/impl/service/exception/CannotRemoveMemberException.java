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

package com.hazelcast.raft.impl.service.exception;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.RaftMember;
import com.hazelcast.raft.RaftManagementService;

/**
 * Thrown while a CP node is requested to be removed from the CP sub-system while there is an ongoing process for another member.
 * This exception will be handled internally and will not be exposed to the user.
 *
 * @see RaftManagementService#triggerRemoveRaftMember(RaftMember)
 */
public class CannotRemoveMemberException extends HazelcastException {

    public CannotRemoveMemberException(String message) {
        super(message, null);
    }

}
