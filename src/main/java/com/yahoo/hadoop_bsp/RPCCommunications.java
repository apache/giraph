package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RPCCommunications<I extends Writable, M extends Writable>
                   extends BasicRPCCommunications<I, M, Object> {
	
    /** Class logger */
    public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

    public RPCCommunications(Context context, CentralizedService<I> service)
            throws IOException, UnknownHostException, InterruptedException {
        super(context, service);
    }

    protected Object createJobToken() throws IOException {
        return null;
    }

    protected Server getRPCServer(
            InetSocketAddress myAddress, int numHandlers, String jobId,
            Object jt) throws IOException {
        return RPC.getServer(this, myAddress.getHostName(),
                myAddress.getPort(), numHandlers, false, conf);
    }

    protected CommunicationsInterface<I, M> getRPCProxy(
            final InetSocketAddress addr, String jobId, Object jt)
            throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        CommunicationsInterface<I, M> proxy =
            (CommunicationsInterface<I, M>) RPC.getProxy(CommunicationsInterface.class,
                                                         versionID, addr, conf);
        return proxy;
    }
}
