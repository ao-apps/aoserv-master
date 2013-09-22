/*
 * Copyright 2012-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.IpReputationSet;
import com.aoindustries.aoserv.client.IpReputationSetHost;
import com.aoindustries.aoserv.client.IpReputationSetNetwork;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.sql.ObjectFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * The <code>IpReputationSetHandler</code> handles all the accesses to the
 * <code>ip_reputation_sets</code> tables.
 *
 * @author  AO Industries, Inc.
 */
final public class IpReputationSetHandler {

    /**
     * Make no instances.
     */
    private IpReputationSetHandler() {
    }

    public static AccountingCode getBusinessForIpReputationSet(DatabaseConnection conn, int ipReputationSet) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select accounting from ip_reputation_sets where pkey=?",
            ipReputationSet
        );
    }

    public static void checkAccessIpReputationSet(DatabaseConnection conn, RequestSource source, String action, int ipReputationSet) throws IOException, SQLException {
        MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
        if(mu!=null) {
            if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) {
                // Must be an admin or router to submit reputation
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access ip reputation set: action='"
                    +action
                    +", pkey="
                    +ipReputationSet
                ;
                throw new SQLException(message);
            }
        } else {
            BusinessHandler.checkAccessBusiness(
                conn,
                source,
                action,
                getBusinessForIpReputationSet(conn, ipReputationSet)
            );
        }
    }

    /**
     * Adds IP reputation with security checks.
     */
    public static void addIpReputation(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ipReputationSet,
        IpReputationSet.AddReputation[] addReputations
    ) throws IOException, SQLException {
        checkAccessIpReputationSet(conn, source, "addIpReputation", ipReputationSet);

        addIpReputation(conn, invalidateList, ipReputationSet, addReputations);
    }

    /**
     * Gets the network for a host and networkPrefix.
     */
    private static int getNetwork(int host, short networkPrefix) {
        return
            host
            & (0xffffffff << (32-networkPrefix))
        ;
    }

    /**
     * Locks all IP reputation tables for updates.  Locks in the same order to avoid any potential deadlock.
     */
    private static void lockForUpdate(
        DatabaseConnection conn
    ) throws SQLException {
        conn.executeUpdate("LOCK TABLE ip_reputation_sets, ip_reputation_set_hosts, ip_reputation_set_networks IN EXCLUSIVE MODE");
    }

    /* TODO: Do in batches
    private static void createTempTable(DatabaseConnection conn, String suffix) throws SQLException {
        conn.executeUpdate(
            "CREATE TEMPORARY TABLE add_reputation_" + suffix + " (\n"
            + "  host INTEGER NOT NULL,\n"
            + "  network INTEGER NOT NULL,\n"
            + "  confidence CHAR(1) NOT NULL,\n"
            + "  score SMALLINT NOT NULL\n"
            + ")"
        );
    }

    private static void addTempRows(
        DatabaseConnection conn,
        short networkPrefix,
        IpReputationSet.AddReputation[] addReputations,
        IpReputationSet.ReputationType reputationType,
        String suffix
    ) throws SQLException {
        PreparedStatement pstmt = conn.getConnection(
            Connection.TRANSACTION_READ_COMMITTED,
            false
        ).prepareStatement("INSERT INTO add_reputation_" + suffix + " VALUES (?,?,?,?)");
        try {
            boolean hasRow = false;
            for(IpReputationSet.AddReputation addRep : addReputations) {
                if(addRep.getReputationType()==reputationType) {
                    hasRow = true;
                    int host = addRep.getHost();
                    pstmt.setInt   (1, host);
                    pstmt.setInt   (2, getNetwork(host, networkPrefix));
                    pstmt.setString(3, Character.toString(addRep.getConfidence().toChar()));
                    pstmt.setShort (4, addRep.getScore());
                    pstmt.addBatch();
                }
            }
            if(hasRow) pstmt.executeBatch();
        } catch(SQLException e) {
            throw new WrappedSQLException(e, pstmt);
        } finally {
            pstmt.close();
            pstmt = null;
        }
    }
     */

    /**
     * Updates the host reputations for any that exist.
     * Adds the new hosts that did not exist.
     *
     * @return  true if any updated
     */
    /* TODO: Do in batches
    private static boolean updateHosts(
        DatabaseConnection conn,
        String suffix,
        short maxUncertainReputation,
        short maxDefiniteReputation
    ) throws SQLException {
        throw new SQLException("TODO: Implement method");
    }
     */

    /**
     * Updates the network reputations for any that exist.
     * Adds the new networks that did not exist.
     *
     * @return  true if any updated
     */
    /* TODO: Do in batches
    private static boolean updateNetworks(
        DatabaseConnection conn,
        String suffix,
        short networkPrefix,
        short maxNetworkReputation
    ) throws SQLException {
        // Determine the maximum score that will result in maxNetworkReputation when divided by network size
        final int maxScore = ((maxNetworkReputation + 1) << (32 - networkPrefix)) - 1;
        conn.execute
        throw new SQLException("TODO: Implement method");
    }
     */
    
    private static short constrainReputation(int newReputation, IpReputationSet.ConfidenceType confidence, short maxUncertainReputation, short maxDefiniteReputation) {
        if(confidence==IpReputationSet.ConfidenceType.UNCERTAIN) {
            return newReputation>maxUncertainReputation ? maxUncertainReputation : (short)newReputation;
        } else if(confidence==IpReputationSet.ConfidenceType.DEFINITE) {
            return newReputation>maxDefiniteReputation ? maxDefiniteReputation : (short)newReputation;
        } else {
            throw new AssertionError("Unexpected value for confidence: " + confidence);
        }
    }

    /**
     * Adds IP reputation with no security checks.
     */
    public static void addIpReputation(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int ipReputationSet,
        IpReputationSet.AddReputation[] addReputations
    ) throws IOException, SQLException {
        // Can't add reputation to a disabled business
        AccountingCode accounting = getBusinessForIpReputationSet(conn, ipReputationSet);
        if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add IP reputation, business disabled: "+accounting);

        if(addReputations.length>0) {
            // Get the settings
            final short maxUncertainReputation = conn.executeShortQuery("SELECT max_uncertain_reputation FROM ip_reputation_sets WHERE pkey=?", ipReputationSet);
            final short maxDefiniteReputation  = conn.executeShortQuery("SELECT max_definite_reputation  FROM ip_reputation_sets WHERE pkey=?", ipReputationSet);
            final short networkPrefix          = conn.executeShortQuery("SELECT network_prefix           FROM ip_reputation_sets WHERE pkey=?", ipReputationSet);
            final short maxNetworkReputation   = conn.executeShortQuery("SELECT max_network_reputation   FROM ip_reputation_sets WHERE pkey=?", ipReputationSet);
            final int   maxNetworkCounter        = ((maxNetworkReputation + 1) << (32 - networkPrefix)) - 1;

            // Will only send signals when changed
            boolean hostsUpdated = false;
            boolean networksUpdated = false;

            // <editor-fold desc="Non-batched">

            // Lock for update
            lockForUpdate(conn);

            // Flag as rep added
            conn.executeUpdate("UPDATE ip_reputation_sets SET last_reputation_added=now() WHERE pkey=?", ipReputationSet);

            for(IpReputationSet.AddReputation addRep : addReputations) {
                int host = addRep.getHost();
                IpReputationSet.ConfidenceType confidence = addRep.getConfidence();
                IpReputationSet.ReputationType reputationType = addRep.getReputationType();
                short score = addRep.getScore();
                IpReputationSetHost dbHost = conn.executeObjectQuery(
                    Connection.TRANSACTION_READ_COMMITTED,
                    true,
                    false,
                    new ObjectFactory<IpReputationSetHost>() {
						@Override
                        public IpReputationSetHost createObject(ResultSet result) throws SQLException {
                            IpReputationSetHost obj = new IpReputationSetHost();
                            obj.init(result);
                            return obj;
                        }
                    },
                    "select * from ip_reputation_set_hosts where \"set\"=? and host=?",
                    ipReputationSet,
                    host
                );
                int positiveChange = 0;
                if(dbHost==null) {
                    // Add new
                    short goodReputation = 0;
                    short badReputation = 0;
                    // Constraint score by confidence
                    short constrainedReputation = constrainReputation(score, confidence, maxUncertainReputation, maxDefiniteReputation);
                    // Resolve starting reputation
                    if(reputationType==IpReputationSet.ReputationType.GOOD) {
                        goodReputation = constrainedReputation;
                        // Update positiveChange for network reputation
                        positiveChange = goodReputation;
                    } else if(reputationType==IpReputationSet.ReputationType.BAD) {
                        badReputation = constrainedReputation;
                    } else {
                        throw new AssertionError("Unexpected value for reputationType: " + reputationType);
                    }
                    if(goodReputation!=0 || badReputation!=0) {
                        int rowCount = conn.executeUpdate(
                            "INSERT INTO ip_reputation_set_hosts (\"set\", host, good_reputation, bad_reputation) VALUES (?,?,?,?)",
                            ipReputationSet,
                            host,
                            goodReputation,
                            badReputation
                        );
                        if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
                        hostsUpdated = true;
                    }
                } else {
                    if(reputationType==IpReputationSet.ReputationType.GOOD) {
                        short oldGoodReputation = dbHost.getGoodReputation();
                        short newGoodReputation = constrainReputation(
                            (int)oldGoodReputation + (int)score,
                            confidence,
                            maxUncertainReputation,
                            maxDefiniteReputation
                        );
                        if(newGoodReputation!=oldGoodReputation) {
                            int rowCount = conn.executeUpdate(
                                "UPDATE ip_reputation_set_hosts SET good_reputation=? WHERE \"set\"=? AND host=?",
                                newGoodReputation,
                                ipReputationSet,
                                host
                            );
                            if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
                            hostsUpdated = true;
                            // Update positiveChange for network reputation
                            positiveChange = newGoodReputation - oldGoodReputation;
                        }
                    } else if(reputationType==IpReputationSet.ReputationType.BAD) {
                        short oldBadReputation = dbHost.getBadReputation();
                        short newBadReputation = constrainReputation(
                            (int)oldBadReputation + (int)score,
                            confidence,
                            maxUncertainReputation,
                            maxDefiniteReputation
                        );
                        if(newBadReputation!=oldBadReputation) {
                            int rowCount = conn.executeUpdate(
                                "UPDATE ip_reputation_set_hosts SET bad_reputation=? WHERE \"set\"=? AND host=?",
                                newBadReputation,
                                ipReputationSet,
                                host
                            );
                            if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
                            hostsUpdated = true;
                        }
                    } else {
                        throw new AssertionError("Unexpected value for reputationType: " + reputationType);
                    }
                }
                if(positiveChange>0) {
                    // Update network when positive change applied
                    int network = getNetwork(host, networkPrefix);
                    IpReputationSetNetwork dbNetwork = conn.executeObjectQuery(
                        Connection.TRANSACTION_READ_COMMITTED,
                        true,
                        false,
                        new ObjectFactory<IpReputationSetNetwork>() {
							@Override
                            public IpReputationSetNetwork createObject(ResultSet result) throws SQLException {
                                IpReputationSetNetwork obj = new IpReputationSetNetwork();
                                obj.init(result);
                                return obj;
                            }
                        },
                        "select * from ip_reputation_set_networks where \"set\"=? and network=?",
                        ipReputationSet,
                        network
                    );
                    if(dbNetwork==null) {
                        // Add new
                        int networkCounter = positiveChange;
                        if(networkCounter>maxNetworkCounter) networkCounter = maxNetworkCounter;
                        int rowCount = conn.executeUpdate(
                            "INSERT INTO ip_reputation_set_networks (\"set\", network, counter) VALUES (?,?,?)",
                            ipReputationSet,
                            network,
                            networkCounter
                        );
                        if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
                        networksUpdated = true;
                    } else {
                        // Update existing
                        int oldCounter = dbNetwork.getCounter();
                        long newCounterLong = (long)oldCounter + (long)positiveChange;
                        int newCounter = newCounterLong <= maxNetworkCounter ? (int)newCounterLong : maxNetworkCounter;
                        if(newCounter!=oldCounter) {
                            int rowCount = conn.executeUpdate(
                                "UPDATE ip_reputation_set_networks SET counter=? WHERE \"set\"=? AND network=?",
                                newCounter,
                                ipReputationSet,
                                network
                            );
                            if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
                            networksUpdated = true;
                        }
                    }
                }
            }
            // </editor-fold>

            // <editor-fold desc="Batched">
            /* TODO: Do in batches
            try {
                // Populate temporary tables
                createTempTable(conn, "good");
                createTempTable(conn, "bad");

                // Add all rows in big batches
                addTempRows(conn, networkPrefix, addReputations, IpReputationSet.ReputationType.GOOD, "good");
                addTempRows(conn, networkPrefix, addReputations, IpReputationSet.ReputationType.BAD,  "bad");

                // Lock for update
                lockForUpdate(conn);

                // Update hosts
                if(updateHosts(conn, "good", maxUncertainReputation, maxDefiniteReputation)) hostsUpdated = true;
                if(updateHosts(conn, "bad" , maxUncertainReputation, maxDefiniteReputation)) hostsUpdated = true;

                // Update networks (only good)
                if(updateNetworks(conn, "good", networkPrefix, maxNetworkReputation)) networksUpdated = true;
                //if(updateNetworks(conn, "bad" , networkPrefix, maxNetworkReputation)) networksUpdated = true;
            } finally {
                conn.executeUpdate("DROP TABLE IF EXISTS add_reputation_good");
                conn.executeUpdate("DROP TABLE IF EXISTS add_reputation_bad");
            }
             */
            // </editor-fold>

            // Notify all clients of the update
            if(hostsUpdated) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.IP_REPUTATION_SET_HOSTS,
                    accounting,
                    BusinessHandler.getServersForBusiness(conn, accounting),
                    false
                );
            }
            if(networksUpdated) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.IP_REPUTATION_SET_NETWORKS,
                    accounting,
                    BusinessHandler.getServersForBusiness(conn, accounting),
                    false
                );
            }
            // Also notify routers
            for(Map.Entry<String,MasterUser> entry : MasterServer.getMasterUsers(conn).entrySet()) {
                String username = entry.getKey();
                MasterUser mu = entry.getValue();
                if(mu.isRouter()) {
                    // TODO: Filter isRouter users by server_farm
                    for(com.aoindustries.aoserv.client.MasterServer ms : MasterServer.getMasterServers(conn, username)) {
                        if(hostsUpdated) {
                            invalidateList.addTable(
                                conn,
                                SchemaTable.TableID.IP_REPUTATION_SET_HOSTS,
                                InvalidateList.allBusinesses,
                                ms.getServerPKey(),
                                false
                            );
                        }
                        if(networksUpdated) {
                            invalidateList.addTable(
                                conn,
                                SchemaTable.TableID.IP_REPUTATION_SET_NETWORKS,
                                InvalidateList.allBusinesses,
                                ms.getServerPKey(),
                                false
                            );
                        }
                    }
                }
            }
        }
    }

    // TODO: CronJob that decays and cleans reputation
    // TODO: Enforce max_hosts here instead of when each batch of hosts is updated?
}
