package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;

/**
 * Struct for failure response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendSuccessResponse
 */
public class AppendFailureResponse implements IdentifiedDataSerializable {

    private RaftEndpoint follower;
    private int term;
    private int expectedNextIndex;

    public AppendFailureResponse() {
    }

    public AppendFailureResponse(RaftEndpoint follower, int term, int expectedNextIndex) {
        this.follower = follower;
        this.term = term;
        this.expectedNextIndex = expectedNextIndex;
    }

    public RaftEndpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public int expectedNextIndex() {
        return expectedNextIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_FAILURE_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeInt(expectedNextIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        expectedNextIndex = in.readInt();
    }

    @Override
    public String toString() {
        return "AppendFailureResponse{" + "follower=" + follower + ", term=" + term + ", expectedNextIndex="
                + expectedNextIndex + '}';
    }

}
