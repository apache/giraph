package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public class RPCCommunications<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends BasicRPCCommunications<I, V, E, M, Object> {

    public RPCCommunications(Context context,
                             CentralizedServiceWorker<I, V, E, M> service)
            throws IOException, UnknownHostException, InterruptedException {
        super(context, service);
    }

    @Override
    protected Object createJobToken() throws IOException {
        return null;
    }

    @Override
    protected Server getRPCServer(
            InetSocketAddress myAddress, int numHandlers, String jobId,
            Object jt) throws IOException {
        return RPC.getServer(this, myAddress.getHostName(),
                myAddress.getPort(), numHandlers, false, conf);
    }

    @Override
    protected CommunicationsInterface<I, V, E, M> getRPCProxy(
            final InetSocketAddress addr, String jobId, Object jt)
            throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        CommunicationsInterface<I, V, E, M> proxy =
            (CommunicationsInterface<I, V, E, M>) RPC.getProxy(
                CommunicationsInterface.class,
                versionID, addr, conf);
        return proxy;
    }
}
