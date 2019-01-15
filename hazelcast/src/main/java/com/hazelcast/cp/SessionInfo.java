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

import com.hazelcast.nio.Address;

/**
 * Represents state of a Raft session.
 */
public interface SessionInfo {

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
     * Returns version of the session. A session's version is incremented on each heartbeat.
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
