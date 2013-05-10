/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.operations.clientv2;

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.collection.operations.MultiMapOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * @ali 5/10/13
 */
public class ValuesRequest extends CollectionAllPartitionRequest {

    public ValuesRequest() {
    }

    public ValuesRequest(CollectionProxyId proxyId) {
        super(proxyId);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(proxyId, MultiMapOperationFactory.OperationFactoryType.VALUES);
    }

    protected Object reduce(Map<Integer, Object> map) {
        Collection list = new LinkedList();
        for (Object obj : map.values()) {
            if (obj == null) {
                continue;
            }
            CollectionResponse response = (CollectionResponse)obj;
            Collection<CollectionRecord> coll = response.getCollection();
            for (CollectionRecord record: coll){
                Object o = getClientEngine().getSerializationService().toObject((Data)record.getObject());
                list.add(o);
            }
        }
        return list;
    }

    public int getClassId() {
        return CollectionPortableHook.VALUES;
    }
}
