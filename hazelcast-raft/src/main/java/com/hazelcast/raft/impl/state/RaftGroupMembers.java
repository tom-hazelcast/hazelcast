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

package com.hazelcast.raft.impl.state;

import com.hazelcast.cp.RaftMember;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Immutable container for members of a Raft group with an index identifying
 * membership change's position in the Raft log.
 */
public class RaftGroupMembers {

    private final long index;

    private final Collection<RaftMember> members;

    private final Collection<RaftMember> remoteMembers;

    RaftGroupMembers(long index, Collection<RaftMember> endpoints, RaftMember localEndpoint) {
        this.index = index;
        this.members = unmodifiableSet(new LinkedHashSet<RaftMember>(endpoints));
        Set<RaftMember> remoteMembers = new LinkedHashSet<RaftMember>(endpoints);
        remoteMembers.remove(localEndpoint);
        this.remoteMembers = unmodifiableSet(remoteMembers);
    }

    /**
     * Returns the position of the membership change that leads to formation of this group.
     */
    public long index() {
        return index;
    }

    /**
     * Return all members in this group.
     *
     * @see #remoteMembers()
     */
    public Collection<RaftMember> members() {
        return members;
    }

    /**
     * Returns remote members in this group, excluding the local member.
     */
    public Collection<RaftMember> remoteMembers() {
        return remoteMembers;
    }

    /**
     * Returns the number of members in this group.
     */
    public int memberCount() {
        return members.size();
    }

    /**
     * Returns the majority for this group.
     */
    public int majority() {
        return members.size() / 2 + 1;
    }

    /**
     * Returns true if the endpoint is a member of this group, false otherwise.
     */
    public boolean isKnownMember(RaftMember endpoint) {
        return members.contains(endpoint);
    }

    @Override
    public String toString() {
        return "RaftGroupMembers{" + "index=" + index + ", members=" + members + '}';
    }

}
