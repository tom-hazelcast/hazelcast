package com.hazelcast.raft.impl.service;

import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftDataService implements SnapshotAwareService<Map<Integer, Object>> {

    public static final String SERVICE_NAME = "RaftTestService";

    private final Map<Integer, Object> values = new ConcurrentHashMap<Integer, Object>();

    public RaftDataService() {
    }

    Object apply(int commitIndex, Object value) {
        assert !values.containsKey(commitIndex) :
                "Cannot apply " + value + "since commitIndex: " + commitIndex + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(int commitIndex) {
        return values.get(commitIndex);
    }

    public int size() {
        return values.size();
    }

    public Set<Object> values() {
        return new HashSet<Object>(values.values());
    }

    @Override
    public Map<Integer, Object> takeSnapshot(RaftGroupId raftGroupId, int commitIndex) {
        Map<Integer, Object> snapshot = new HashMap<Integer, Object>();
        for (Entry<Integer, Object> e : values.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            snapshot.put(e.getKey(), e.getValue());
        }

        return snapshot;
    }

    @Override
    public void restoreSnapshot(RaftGroupId raftGroupId, int commitIndex, Map<Integer, Object> snapshot) {
        values.clear();
        values.putAll(snapshot);
    }
}