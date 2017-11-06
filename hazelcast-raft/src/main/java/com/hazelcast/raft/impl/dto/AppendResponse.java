package com.hazelcast.raft.impl.dto;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class AppendResponse implements IdentifiedDataSerializable {

    public Address follower;
    public int term;
    public boolean success;
    public int lastLogIndex;

    public AppendResponse() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeInt(term);
        out.writeObject(follower);
        out.writeInt(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        success = in.readBoolean();
        term = in.readInt();
        follower = in.readObject();
        lastLogIndex = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_RESPONSE;
    }

    @Override
    public String toString() {
        return "AppendResponse{" + "follower=" + follower + ", term=" + term + ", success=" + success + ", lastLogIndex="
                + lastLogIndex + '}';
    }

}