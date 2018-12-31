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

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.nio.Address;

/**
 * Represents a CP session.
 *
 * For CP data structures that are doing ownership management of resources,
 * such as lock or semaphore, a session is required to keep track liveliness
 * of the caller. In this context, caller means an entity that uses
 * the CPI subsystem APIs. It can be either a Hazelcast server or a client.
 * A caller initially creates a session before sending its very first
 * session-based request to the CP group, such as lock / semaphore acquire.
 * After creating a session on the CP group, the caller stores its session id
 * locally and sends it alongside its session-based operations. A single
 * session is used by multiple lock and semaphore instances. When a CP group
 * receives a session-based operation, it checks validity of the session using
 * the session id information available in the operation. An operation with a
 * valid session id is accepted as a session heartbeat. While a caller is idle,
 * in other words, it does not send any session-based operation to the CP group
 * for a while, it commits periodic heartbeats to the CP group in the background
 * in order to keep its session alive. This interval is specified in
 * {@link CPSubsystemConfig#getSessionHeartbeatIntervalSeconds()}.
 * <p>
 * A session is closed when the caller does not touch the session during
 * a predefined duration. In this case, the caller is assumed to be crashed and
 * all its resources are released automatically. This duration is specified in
 * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}.
 * <p>
 * Sessions offer a trade-off between liveliness and safety. If a very small
 * value is set to {@link CPSubsystemConfig#setSessionTimeToLiveSeconds(int)},
 * then a session owner could be considered crashed very quickly and its
 * resources can be released prematurely. On the other hand, if a large value
 * is set, a session could be kept alive for an unnecessarily long duration
 * even if its owner actually crashes.
 */
public interface CPSession {

    /**
     * Returns id of the session
     */
    long id();

    /**
     * Returns the timestamp of when the session was created
     */
    long creationTime();

    /**
     * Returns the timestamp of when the session will expire
     */
    long expirationTime();

    /**
     * Returns version of the session.
     * A session's version is incremented on each heartbeat.
     */
    long version();

    /**
     * Returns true if the session expires before the given timestamp.
     */
    boolean isExpired(long timestamp);

    /**
     * Returns the endpoint that has created this session
     */
    Address endpoint();
}
