/*
 * Copyright 2000-2010 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.rmi.RmiServerConnectorFactory;
import com.aoindustries.aoserv.client.validator.Hostname;
import com.aoindustries.aoserv.client.validator.InetAddress;
import com.aoindustries.aoserv.client.validator.NetPort;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.aoserv.master.database.DatabaseConnectorFactory;
import com.aoindustries.aoserv.master.threadLocale.ThreadLocaleConnector;
import com.aoindustries.aoserv.master.threadLocale.ThreadLocaleConnectorFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launches all of the server processes.
 */
final public class MasterServer {

    private static final Logger logger = LogFactory.getLogger(MasterServer.class);

    private MasterServer() {
    }

    public static void main(String[] args) {
        // TODO: security manager
        //if(System.getSecurityManager()==null) {
        //    Policy.
        //    System.setSecurityManager(new SecurityManager());
        //}

        // Start the RMI server, if starting other types of servers later, consider starting each on separate threads so
        // a failure in on protocol doesn't affect the start-up of the others.
        System.out.print("Starting RmiServer: ");
        boolean done = false;
        while(!done) {
            try {
                // Get the configuration
                Hostname publicAddress = MasterConfiguration.getRmiPublicAddress();
                InetAddress listenAddress = MasterConfiguration.getRmiListenAddress();
                NetPort port = MasterConfiguration.getRmiPort();
                boolean useSsl = MasterConfiguration.getRmiUseSsl();
                UserId rootUsername = MasterConfiguration.getRootUsername();
                String rootPassword = MasterConfiguration.getRootPassword();
                if(useSsl) {
                    String keystorePath = MasterConfiguration.getSSLKeystorePath();
                    String keystorePassword = MasterConfiguration.getSSLKeystorePassword();
                    if(keystorePath==null) throw new IllegalArgumentException("keystorePath is required when useSsl is true");
                    if(keystorePassword==null) throw new IllegalArgumentException("keystorePassword is required when useSsl is true");
                    if(System.getProperty("javax.net.ssl.keyStore")==null) {
                        System.setProperty(
                            "javax.net.ssl.keyStore",
                            keystorePath
                        );
                    }
                    if(System.getProperty("javax.net.ssl.keyStorePassword")==null) {
                        System.setProperty(
                            "javax.net.ssl.keyStorePassword",
                            keystorePassword
                        );
                    }
                }

                // Start the RMI server
                RmiServerConnectorFactory<ThreadLocaleConnector,ThreadLocaleConnectorFactory> factory = new RmiServerConnectorFactory<ThreadLocaleConnector,ThreadLocaleConnectorFactory>(
                    publicAddress,
                    listenAddress,
                    port,
                    useSsl,
                    new ThreadLocaleConnectorFactory(new DatabaseConnectorFactory(MasterDatabase.getDatabase(), rootUsername, rootPassword))
                );
                done = true;
                System.out.println("Done");

                // Avoid bug: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6597112
                try {
                    Thread.sleep(300000); // Wait five minutes before losing reference to factory.
                } catch(InterruptedException err) {
                    logger.log(Level.WARNING, null, err);
                }
            } catch(Exception err) {
                logger.log(Level.SEVERE, null, err);
                try {
                    Thread.sleep(10000);
                } catch(InterruptedException err2) {
                    logger.log(Level.WARNING, null, err2);
                }
            }
        }
    }
}
