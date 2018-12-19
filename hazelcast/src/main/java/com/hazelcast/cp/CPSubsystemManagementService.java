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

package com.hazelcast.cp;

import com.hazelcast.core.EndpointIdentifier;
import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * The public API used for managing CP members and groups.
 */
public interface CPSubsystemManagementService {

    /**
     * Returns all CP group ids.
     */
    Collection<CPGroupId> getCPGroupIds();

    /**
     * Returns the CP group associated with the group id.
     */
    CPGroup getCPGroup(CPGroupId groupId);

    /**
     * Wipes & resets the local CP state and initializes it as if this node starting up initially.
     * This method must be used only when the Metadata CP group loses its majority and cannot make progress anymore.
     * <p>
     * After this method is called, all CP state and data will be wiped and CP members will start empty.
     * <p>
     * <strong>Use with caution:
     * This method is NOT idempotent and multiple invocations on the same member can break the whole system!</strong>
     */
    void resetAndInit();

    /**
     * Promotes the local Hazelcast cluster member to a CP member.
     * <p>
     * This method is idempotent.
     * If the local member is in the active CP members list already, then this method will have no effect.
     * <p>
     * If the local member is currently being removed from the active CP members list,
     * then the returning Future object will throw {@link IllegalArgumentException}.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalArgumentException If the local member is currently being removed from the active CP members list
     */
    ICompletableFuture<Void> promoteToCPMember();

    /**
     * Removes the given unreachable member from the active CP members list and all CP groups it belongs to.
     * If other active CP members are available, they will replace the removed member in CP groups.
     * Otherwise, CP groups which the removed member is a member of will shrink
     * and their majority values will be recalculated.
     * <p>
     * This method can be invoked only from the Hazelcast master node.
     * <p>
     * This method is idempotent.
     * If the given member is not in the active CP members list, then this method will have no effect.
     *
     * @return a Future representing pending completion of the operation
     * @throws IllegalStateException When member removal initiated by a non-master member
     *                               or the given member is still member of the Hazelcast cluster
     *                               or another CP member is being removed from the CP sub-system
     */
    ICompletableFuture<Void> removeCPMember(EndpointIdentifier member);

    /**
     * Unconditionally destroys the given CP group without using the Raft algorithm mechanics.
     * This method must be used only when the given CP group loses its majority and cannot make progress anymore.
     * Normally, membership changes in CP groups are done via the Raft algorithm.
     * However, this method forcefully terminates the remaining nodes of the given CP group.
     * It also performs a Raft commit to the Metadata group in order to update status of the destroyed group.
     * <p>
     * This method is idempotent. It has no effect if the given CP group is already destroyed.
     *
     * @return a Future representing pending completion of the operation
     */
    ICompletableFuture<Void> forceDestroyCPGroup(CPGroupId groupId);
}
