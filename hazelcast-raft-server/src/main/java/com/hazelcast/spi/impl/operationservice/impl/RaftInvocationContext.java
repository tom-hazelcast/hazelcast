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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.exception.RaftException;
import com.hazelcast.raft.impl.RaftMemberImpl;
import com.hazelcast.raft.impl.service.RaftGroupInfo;
import com.hazelcast.raft.impl.service.RaftService;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO: Javadoc Pending...
 *
 * @author mdogan 30.05.2018
 */
public class RaftInvocationContext {
    private final ILogger logger;

    private final RaftService raftService;
    private final ConcurrentMap<RaftGroupId, RaftMemberImpl> knownLeaders =
            new ConcurrentHashMap<RaftGroupId, RaftMemberImpl>();
    final boolean failOnIndeterminateOperationState;

    private volatile RaftMemberImpl[] allEndpoints = {};

    public RaftInvocationContext(ILogger logger, RaftService raftService) {
        this.logger = logger;
        this.raftService = raftService;
        this.failOnIndeterminateOperationState = raftService.getConfig()
                .getRaftConfig().isFailOnIndeterminateOperationState();
    }

    public void reset() {
        allEndpoints = new RaftMemberImpl[0];
        knownLeaders.clear();
    }

    public void setAllEndpoints(Collection<RaftMemberImpl> endpoints) {
        allEndpoints = endpoints.toArray(new RaftMemberImpl[0]);
    }

    public RaftMemberImpl getKnownLeader(RaftGroupId groupId) {
        return knownLeaders.get(groupId);
    }

    void setKnownLeader(RaftGroupId groupId, RaftMemberImpl leader) {
        logger.fine("Setting known leader for raft: " + groupId + " to " + leader);
        knownLeaders.put(groupId, leader);
    }

    void updateKnownLeaderOnFailure(RaftGroupId groupId, Throwable cause) {
        if (cause instanceof RaftException) {
            RaftException e = (RaftException) cause;
            RaftMemberImpl leader = (RaftMemberImpl) e.getLeader();
            if (leader != null) {
                setKnownLeader(groupId, leader);
            } else {
                resetKnownLeader(groupId);
            }
        } else {
            resetKnownLeader(groupId);
        }
    }

    private void resetKnownLeader(RaftGroupId groupId) {
        logger.fine("Resetting known leader for raft: " + groupId);
        knownLeaders.remove(groupId);
    }

    EndpointCursor newEndpointCursor(RaftGroupId groupId) {
        RaftGroupInfo group = raftService.getRaftGroup(groupId);
        RaftMemberImpl[] endpoints = group != null ? group.membersArray() : allEndpoints;
        return new EndpointCursor(endpoints);
    }

    static class EndpointCursor {
        private final RaftMemberImpl[] endpoints;
        private int index = -1;

        private EndpointCursor(RaftMemberImpl[] endpoints) {
            this.endpoints = endpoints;
        }

        boolean advance() {
            return ++index < endpoints.length;
        }

        RaftMemberImpl get() {
            return endpoints[index];
        }
    }
}
