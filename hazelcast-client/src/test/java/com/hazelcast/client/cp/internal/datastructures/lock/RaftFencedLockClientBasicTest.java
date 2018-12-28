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

package com.hazelcast.client.cp.internal.datastructures.lock;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.datastructures.lock.RaftFencedLockBasicTest;
import com.hazelcast.cp.internal.session.AbstractProxySessionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftFencedLockClientBasicTest extends RaftFencedLockBasicTest {

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = super.createInstances();
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        lockInstance = f.newHazelcastClient();
        return instances;
    }

    @Test
    public void test_lockAutoRelease_onClientShutdown() {
        String proxyName = lock.getName();
        lock.lock();

        lockInstance.shutdown();

        assertFalse(instances[0].getCPSubsystem().getLock(proxyName).isLocked());
    }

    protected AbstractProxySessionManager getSessionManager(HazelcastInstance instance) {
        return (((HazelcastClientProxy) lockInstance).client).getProxySessionManager();
    }

}
