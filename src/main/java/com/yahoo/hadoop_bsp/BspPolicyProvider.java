package com.yahoo.hadoop_bsp;

import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
  * {@link PolicyProvider} for Map-Reduce protocols.
  */
public class BspPolicyProvider extends PolicyProvider {
    private static final Service[] bspCommunicationsServices =
        new Service[] {
            new Service("security.bsp.communications.protocol.acl",
                                    CommunicationsInterface.class),
    };

    @Override
    public Service[] getServices() {
        return bspCommunicationsServices;
    }
}

