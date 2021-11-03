/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2012, 2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of aoserv-master.
 *
 * aoserv-master is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * aoserv-master is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.sql.Connections;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.net.reputation.Host;
import com.aoindustries.aoserv.client.net.reputation.Network;
import com.aoindustries.aoserv.client.net.reputation.Set;
import com.aoindustries.aoserv.client.schema.Table;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * The <code>IpReputationSetHandler</code> handles all the accesses to the
 * <code>net.reputation.Set</code> tables.
 *
 * @author  AO Industries, Inc.
 */
public final class NetReputationSetHandler {

	/**
	 * Make no instances.
	 */
	private NetReputationSetHandler() {
	}

	public static Account.Name getAccountForIpReputationSet(DatabaseConnection conn, int ipReputationSet) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select accounting from \"net.reputation\".\"Set\" where id=?",
			ipReputationSet
		);
	}

	public static void checkAccessIpReputationSet(DatabaseConnection conn, RequestSource source, String action, int ipReputationSet) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length!=0) {
				// Must be an admin or router to submit reputation
				String message=
					"currentAdministrator="
					+source.getCurrentAdministrator()
					+" is not allowed to access ip reputation set: action='"
					+action
					+", id="
					+ipReputationSet
				;
				throw new SQLException(message);
			}
		} else {
			AccountHandler.checkAccessAccount(conn,
				source,
				action,
				getAccountForIpReputationSet(conn, ipReputationSet)
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
		Set.AddReputation[] addReputations
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
		conn.update(
			"LOCK TABLE\n"
			+ "  \"net.reputation\".\"Set\",\n"
			+ "  \"net.reputation\".\"Host\",\n"
			+ "  \"net.reputation\".\"Network\"\n"
			+ "IN EXCLUSIVE MODE");
	}

	/* TODO: Do in batches
	private static void createTempTable(DatabaseConnection conn, String suffix) throws SQLException {
		conn.update(
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
		Set.AddReputation[] addReputations,
		Set.ReputationType reputationType,
		String suffix
	) throws SQLException {
		try (PreparedStatement pstmt = conn.getConnection().prepareStatement("INSERT INTO add_reputation_" + suffix + " VALUES (?,?,?,?)")) {
			boolean hasRow = false;
			for(Set.AddReputation addRep : addReputations) {
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
		} catch(Error | RuntimeException | SQLException e) {
			ErrorPrinter.addSQL(e, pstmt);
			throw e;
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

	private static short constrainReputation(int newReputation, Set.ConfidenceType confidence, short maxUncertainReputation, short maxDefiniteReputation) {
		if(confidence==Set.ConfidenceType.UNCERTAIN) {
			return newReputation>maxUncertainReputation ? maxUncertainReputation : (short)newReputation;
		} else if(confidence==Set.ConfidenceType.DEFINITE) {
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
		Set.AddReputation[] addReputations
	) throws IOException, SQLException {
		// Can't add reputation to a disabled business
		Account.Name account = getAccountForIpReputationSet(conn, ipReputationSet);
		if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to add IP reputation, business disabled: "+account);

		if(addReputations.length>0) {
			// Get the settings
			final short maxUncertainReputation = conn.queryShort("SELECT max_uncertain_reputation FROM \"net.reputation\".\"Set\" WHERE id=?", ipReputationSet);
			final short maxDefiniteReputation  = conn.queryShort("SELECT max_definite_reputation  FROM \"net.reputation\".\"Set\" WHERE id=?", ipReputationSet);
			final short networkPrefix          = conn.queryShort("SELECT network_prefix           FROM \"net.reputation\".\"Set\" WHERE id=?", ipReputationSet);
			final short maxNetworkReputation   = conn.queryShort("SELECT max_network_reputation   FROM \"net.reputation\".\"Set\" WHERE id=?", ipReputationSet);
			final int   maxNetworkCounter        = ((maxNetworkReputation + 1) << (32 - networkPrefix)) - 1;

			// Will only send signals when changed
			boolean hostsUpdated = false;
			boolean networksUpdated = false;

			// <editor-fold desc="Non-batched">

			// Lock for update
			lockForUpdate(conn);

			// Flag as rep added
			conn.update("UPDATE \"net.reputation\".\"Set\" SET last_reputation_added=now() WHERE id=?", ipReputationSet);

			for(Set.AddReputation addRep : addReputations) {
				int host = addRep.getHost();
				Set.ConfidenceType confidence = addRep.getConfidence();
				Set.ReputationType reputationType = addRep.getReputationType();
				short score = addRep.getScore();
				Host dbHost = conn.queryObject(
					Connections.DEFAULT_TRANSACTION_ISOLATION, true, false,
					result -> {
						Host obj = new Host();
						obj.init(result);
						return obj;
					},
					"select * from \"net.reputation\".\"Host\" where \"set\"=? and host=?",
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
					if(reputationType==Set.ReputationType.GOOD) {
						goodReputation = constrainedReputation;
						// Update positiveChange for network reputation
						positiveChange = goodReputation;
					} else if(reputationType==Set.ReputationType.BAD) {
						badReputation = constrainedReputation;
					} else {
						throw new AssertionError("Unexpected value for reputationType: " + reputationType);
					}
					if(goodReputation!=0 || badReputation!=0) {
						int rowCount = conn.update(
							"INSERT INTO \"net.reputation\".\"Host\" (\"set\", host, good_reputation, bad_reputation) VALUES (?,?,?,?)",
							ipReputationSet,
							host,
							goodReputation,
							badReputation
						);
						if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
						hostsUpdated = true;
					}
				} else {
					if(reputationType==Set.ReputationType.GOOD) {
						short oldGoodReputation = dbHost.getGoodReputation();
						short newGoodReputation = constrainReputation(
							(int)oldGoodReputation + (int)score,
							confidence,
							maxUncertainReputation,
							maxDefiniteReputation
						);
						if(newGoodReputation!=oldGoodReputation) {
							int rowCount = conn.update(
								"UPDATE \"net.reputation\".\"Host\" SET good_reputation=? WHERE \"set\"=? AND host=?",
								newGoodReputation,
								ipReputationSet,
								host
							);
							if(rowCount!=1) throw new SQLException("Wrong number of rows updated: " + rowCount);
							hostsUpdated = true;
							// Update positiveChange for network reputation
							positiveChange = newGoodReputation - oldGoodReputation;
						}
					} else if(reputationType==Set.ReputationType.BAD) {
						short oldBadReputation = dbHost.getBadReputation();
						short newBadReputation = constrainReputation(
							(int)oldBadReputation + (int)score,
							confidence,
							maxUncertainReputation,
							maxDefiniteReputation
						);
						if(newBadReputation!=oldBadReputation) {
							int rowCount = conn.update(
								"UPDATE \"net.reputation\".\"Host\" SET bad_reputation=? WHERE \"set\"=? AND host=?",
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
					Network dbNetwork = conn.queryObject(
						Connections.DEFAULT_TRANSACTION_ISOLATION, true, false,
						result -> {
							Network obj = new Network();
							obj.init(result);
							return obj;
						},
						"select * from \"net.reputation\".\"Network\" where \"set\"=? and network=?",
						ipReputationSet,
						network
					);
					if(dbNetwork==null) {
						// Add new
						int networkCounter = positiveChange;
						if(networkCounter>maxNetworkCounter) networkCounter = maxNetworkCounter;
						int rowCount = conn.update(
							"INSERT INTO \"net.reputation\".\"Network\" (\"set\", network, counter) VALUES (?,?,?)",
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
							int rowCount = conn.update(
								"UPDATE \"net.reputation\".\"Network\" SET counter=? WHERE \"set\"=? AND network=?",
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
				addTempRows(conn, networkPrefix, addReputations, Set.ReputationType.GOOD, "good");
				addTempRows(conn, networkPrefix, addReputations, Set.ReputationType.BAD,  "bad");

				// Lock for update
				lockForUpdate(conn);

				// Update hosts
				if(updateHosts(conn, "good", maxUncertainReputation, maxDefiniteReputation)) hostsUpdated = true;
				if(updateHosts(conn, "bad" , maxUncertainReputation, maxDefiniteReputation)) hostsUpdated = true;

				// Update networks (only good)
				if(updateNetworks(conn, "good", networkPrefix, maxNetworkReputation)) networksUpdated = true;
				//if(updateNetworks(conn, "bad" , networkPrefix, maxNetworkReputation)) networksUpdated = true;
			} finally {
				conn.update("DROP TABLE IF EXISTS add_reputation_good");
				conn.update("DROP TABLE IF EXISTS add_reputation_bad");
			}
			 */
			// </editor-fold>

			// Notify all clients of the update
			if(hostsUpdated) {
				invalidateList.addTable(
					conn,
					Table.TableID.IP_REPUTATION_SET_HOSTS,
					account,
					AccountHandler.getHostsForAccount(conn, account),
					false
				);
			}
			if(networksUpdated) {
				invalidateList.addTable(
					conn,
					Table.TableID.IP_REPUTATION_SET_NETWORKS,
					account,
					AccountHandler.getHostsForAccount(conn, account),
					false
				);
			}
			// Also notify routers
			for(Map.Entry<com.aoindustries.aoserv.client.account.User.Name, User> entry : MasterServer.getUsers(conn).entrySet()) {
				com.aoindustries.aoserv.client.account.User.Name user = entry.getKey();
				User mu = entry.getValue();
				if(mu.isRouter()) {
					// TODO: Filter isRouter users by server_farm
					for(UserHost ms : MasterServer.getUserHosts(conn, user)) {
						if(hostsUpdated) {
							invalidateList.addTable(conn,
								Table.TableID.IP_REPUTATION_SET_HOSTS,
								InvalidateList.allAccounts,
								ms.getServerPKey(),
								false
							);
						}
						if(networksUpdated) {
							invalidateList.addTable(conn,
								Table.TableID.IP_REPUTATION_SET_NETWORKS,
								InvalidateList.allAccounts,
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
