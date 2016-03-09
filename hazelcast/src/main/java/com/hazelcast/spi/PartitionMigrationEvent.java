/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.partition.MigrationEndpoint;

import java.util.EventObject;

/**
 * An {@link java.util.EventObject} for a partition migration. Can be used by SPI services to get a callback
 * to listen to partition migration.  See {@link com.hazelcast.spi.MigrationAwareService} for more info.
 */
public class PartitionMigrationEvent extends EventObject {

    private final MigrationEndpoint migrationEndpoint;

    private final int partitionId;

    private final int currentReplicaIndex;

    private final int newReplicaIndex;

//    public PartitionMigrationEvent(MigrationEndpoint migrationEndpoint, int partitionId) {
//        this(migrationEndpoint, partitionId, -1, 0);
//    }

    public PartitionMigrationEvent(MigrationEndpoint migrationEndpoint, int partitionId,
            int currentReplicaIndex, int newReplicaIndex) {
        super(partitionId);
        this.migrationEndpoint = migrationEndpoint;
        this.partitionId = partitionId;
        this.currentReplicaIndex = currentReplicaIndex;
        this.newReplicaIndex = newReplicaIndex;
    }

    /**
     * Gets the partition migration endpoint.
     *
     * @return the partition migration endpoint
     */
    public MigrationEndpoint getMigrationEndpoint() {
        return migrationEndpoint;
    }

    /**
     * Gets the partition id.
     *
     * @return the partition id
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * TODO
     * @return
     */
    public int getCurrentReplicaIndex() {
        return currentReplicaIndex;
    }

    /**
     * TODO
     *
     * @return
     */
    public int getNewReplicaIndex() {
        return newReplicaIndex;
    }

    @Override
    public String toString() {
        return "PartitionMigrationEvent{" +
                "migrationEndpoint=" + migrationEndpoint +
                ", partitionId=" + partitionId +
                ", currentReplicaIndex=" + currentReplicaIndex +
                ", newReplicaIndex=" + newReplicaIndex +
                '}';
    }
}
