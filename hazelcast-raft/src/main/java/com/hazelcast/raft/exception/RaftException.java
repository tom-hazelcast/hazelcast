package com.hazelcast.raft.exception;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.Address;
import com.hazelcast.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * TODO: Javadoc Pending...
 *
 */
public class RaftException extends HazelcastException {

    private transient RaftEndpoint leader;

    public RaftException(RaftEndpoint leader) {
        this.leader = leader;
    }

    public RaftException(String message, RaftEndpoint leader) {
        super(message);
        this.leader = leader;
    }

    public RaftEndpoint getLeader() {
        return leader;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        if (leader == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeEndpoint(leader, out);
        }
    }

    static void writeEndpoint(RaftEndpoint endpoint, ObjectOutputStream out) throws IOException {
        out.writeUTF(endpoint.getUid());
        Address address = endpoint.getAddress();
        out.writeUTF(address.getHost());
        out.writeInt(address.getPort());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (in.readBoolean()) {
            leader = readEndpoint(in);
        }
    }

    static RaftEndpoint readEndpoint(ObjectInputStream in) throws IOException {
        String uid = in.readUTF();
        String host = in.readUTF();
        int port = in.readInt();
        return new RaftEndpoint(uid, new Address(host, port));
    }
}
