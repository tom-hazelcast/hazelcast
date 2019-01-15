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

package com.hazelcast.raft.service.session;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.impl.session.SessionAccessor;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.test.AssertTask;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractSessionManagerTest extends HazelcastRaftTestSupport {

    private static final int sessionTTLSeconds = 5;

    HazelcastInstance[] members;
    private RaftGroupId groupId;

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        int raftGroupSize = 3;
        Address[] raftAddresses = createAddresses(raftGroupSize);
        members = newInstances(raftAddresses, raftGroupSize, 0);

        RaftInvocationManager invocationManager = getRaftInvocationManager(members[0]);
        groupId = invocationManager.createRaftGroup("group", raftGroupSize).get();
    }

    @Test
    public void getSession_returnsNoSessionId_whenNoSessionCreated() {
        AbstractSessionManager sessionManager = getSessionManager();
        assertEquals(AbstractSessionManager.NO_SESSION_ID, sessionManager.getSession(groupId));
    }

    @Test
    public void acquireSession_createsNewSession_whenSessionNotExists() {
        AbstractSessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);
        assertNotEquals(AbstractSessionManager.NO_SESSION_ID, sessionId);
        assertEquals(sessionId, sessionManager.getSession(groupId));
        assertEquals(1, sessionManager.getSessionUsageCount(groupId, sessionId));

        SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrue(sessionAccessor.isValid(groupId, sessionId));
    }

    @Test
    public void acquireSession_returnsExistingSession_whenSessionExists() {
        AbstractSessionManager sessionManager = getSessionManager();
        long newSessionId = sessionManager.acquireSession(groupId);
        long sessionId = sessionManager.acquireSession(groupId);
        assertEquals(newSessionId, sessionId);
        assertEquals(sessionId, sessionManager.getSession(groupId));
        assertEquals(2, sessionManager.getSessionUsageCount(groupId, sessionId));
    }

    @Test
    public void acquireSession_returnsTheSameSessionId_whenExecutedConcurrently() throws Exception {
        final AbstractSessionManager sessionManager = getSessionManager();

        Callable<Long> acquireSessionCall = new Callable<Long>() {
            @Override
            public Long call() {
                return sessionManager.acquireSession(groupId);
            }
        };

        Future<Long>[] futures = new Future[5];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = spawn(acquireSessionCall);
        }

        long[] sessions = new long[futures.length];
        for (int i = 0; i < futures.length; i++) {
            sessions[i] = futures[i].get();
        }

        long expectedSessionId = sessionManager.getSession(groupId);
        for (long sessionId : sessions) {
            assertEquals(expectedSessionId, sessionId);
        }
        assertEquals(sessions.length, sessionManager.getSessionUsageCount(groupId, expectedSessionId));
    }

    @Test
    public void releaseSession_hasNoEffect_whenSessionNotExists() {
        AbstractSessionManager sessionManager = getSessionManager();
        sessionManager.releaseSession(groupId, 1);
    }

    @Test
    public void releaseSession_whenSessionExists() {
        AbstractSessionManager sessionManager = getSessionManager();
        long sessionId = sessionManager.acquireSession(groupId);
        sessionManager.releaseSession(groupId, sessionId);
        assertEquals(0, sessionManager.getSessionUsageCount(groupId, sessionId));
    }

    @Test
    public void sessionHeartbeatsAreNotSent_whenSessionNotExists() {
        final AbstractSessionManager sessionManager = getSessionManager();
        final long sessionId = 1;

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                verify(sessionManager, never()).heartbeat(groupId, sessionId);
            }
        }, 5);
    }

    @Test
    public void sessionHeartbeatsAreSent_whenSessionInUse() {
        final AbstractSessionManager sessionManager = getSessionManager();
        final long sessionId = sessionManager.acquireSession(groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                verify(sessionManager, atLeastOnce()).heartbeat(groupId, sessionId);
            }
        });

        final SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertTrue(sessionAccessor.isValid(groupId, sessionId));
            }
        }, sessionTTLSeconds);
    }

    @Test
    public void sessionHeartbeatsAreNotSent_whenSessionReleased() {
        final AbstractSessionManager sessionManager = getSessionManager();
        final long sessionId = sessionManager.acquireSession(groupId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                verify(sessionManager, atLeastOnce()).heartbeat(groupId, sessionId);
            }
        });

        sessionManager.releaseSession(groupId, sessionId);

        final SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(sessionAccessor.isValid(groupId, sessionId));
            }
        });
    }

    @Test
    public void acquireSession_returnsTheExistingSession_whenSessionInUse() {
        final AbstractSessionManager sessionManager = getSessionManager();

        final long sessionId = sessionManager.acquireSession(groupId);

        when(sessionManager.heartbeat(groupId, sessionId)).thenReturn(completedFuture());

        final SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(sessionAccessor.isValid(groupId, sessionId));
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(sessionId, sessionManager.acquireSession(groupId));
            }
        }, 3);
    }

    @Test
    public void acquireSession_returnsNewSession_whenSessionExpiredAndNotInUse() {
        final AbstractSessionManager sessionManager = getSessionManager();

        final long sessionId = sessionManager.acquireSession(groupId);

        when(sessionManager.heartbeat(groupId, sessionId)).thenReturn(completedFuture());

        final SessionAccessor sessionAccessor = getSessionAccessor();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(sessionAccessor.isValid(groupId, sessionId));
            }
        });

        sessionManager.releaseSession(groupId, sessionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotEquals(sessionId, sessionManager.acquireSession(groupId));
            }
        });
    }

    protected abstract AbstractSessionManager getSessionManager();

    private SimpleCompletableFuture<Object> completedFuture() {
        SimpleCompletableFuture<Object> future = new SimpleCompletableFuture<Object>(new CallerRunsExecutor(), null);
        future.setResult(null);
        return future;
    }

    private SessionAccessor getSessionAccessor() {
        return getNodeEngineImpl(members[0]).getService(RaftSessionService.SERVICE_NAME);
    }

    private static class CallerRunsExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    @Override
    protected Config createConfig(int groupSize, int metadataGroupSize) {
        Config config = super.createConfig(groupSize, metadataGroupSize);
        RaftServiceConfig raftServiceConfig = config.getRaftServiceConfig();
        raftServiceConfig.setSessionHeartbeatIntervalMillis(500);
        raftServiceConfig.setSessionTimeToLiveSeconds(sessionTTLSeconds);
        return config;
    }
}
