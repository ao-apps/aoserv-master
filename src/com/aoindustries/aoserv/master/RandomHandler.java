/*
 * Copyright 2004-2013, 2015, 2017, 2018, 2019 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.dbc.DatabaseConnection;
import com.aoindustries.io.FifoFile;
import com.aoindustries.io.FifoFileInputStream;
import com.aoindustries.io.FifoFileOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.sql.SQLException;

/**
 * The <code>RandomHandler</code> stores obtains a pool of random data from servers that have excess and provides
 * this data to those servers that need more.
 *
 * @author  AO Industries, Inc.
 */
public final class RandomHandler {

	private static FifoFile fifoFile;

	public static FifoFile getFifoFile() throws IOException {
		synchronized(RandomHandler.class) {
			if(fifoFile == null) fifoFile = new FifoFile(MasterConfiguration.getEntropyPoolFilePath(), AOServConnector.MASTER_ENTROPY_POOL_SIZE);
			return fifoFile;
		}
	}

	private static void checkAccessEntropy(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
		boolean isAllowed = false;

		com.aoindustries.aoserv.client.account.User.Name currentAdministrator = source.getCurrentAdministrator();
		User mu = MasterServer.getUser(conn, currentAdministrator);
		if (mu != null) {
			UserHost[] masterServers = MasterServer.getUserHosts(conn, currentAdministrator);
			if(masterServers.length == 0) isAllowed = true;
			else {
				for (UserHost masterServer : masterServers) {
					if (NetHostHandler.isLinuxServer(conn, masterServer.getServerPKey())) {
						isAllowed = true;
						break;
					}
				}
			}
		}
		if(!isAllowed) {
			throw new SQLException(
				"currentAdministrator="
				+ currentAdministrator
				+ " is not allowed to access the master entropy pool: action='"
				+ action
				+ '\''
			);
		}
	}

	public static void addMasterEntropy(
		DatabaseConnection conn,
		RequestSource source,
		byte[] entropy,
		int numBytes
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "addMasterEntropy");

		FifoFile fifo = getFifoFile();
		synchronized(fifo) {
			FifoFileOutputStream fifoOut = fifo.getOutputStream();
			long available = fifoOut.available();
			int addCount = numBytes;
			if(available < addCount) addCount = (int)available;
			if(addCount > 0) {
				fifoOut.write(entropy, 0, addCount);
				fifo.flush();
			}
		}
	}

	public static int getMasterEntropy(
		DatabaseConnection conn,
		RequestSource source,
		byte[] entropy,
		int numBytes
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "getMasterEntropy");

		FifoFile fifo = getFifoFile();
		synchronized(fifo) {
			FifoFileInputStream fifoIn = fifo.getInputStream();
			long available = fifoIn.available();
			if(available < numBytes) numBytes = (int)available;
			if(numBytes > 0) {
				int pos = 0;
				while(pos < numBytes) {
					int ret = fifoIn.read(entropy, pos, numBytes - pos);
					if(ret == -1) throw new EOFException("Unexpected EOF");
					pos += ret;
				}
				fifo.flush();
			}
			return numBytes;
		}
	}

	public static long getMasterEntropyNeeded(
		DatabaseConnection conn,
		RequestSource source
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "getMasterEntropyNeeded");

		FifoFile file = getFifoFile();
		synchronized(file) {
			return file.getOutputStream().available();
		}
	}

	private RandomHandler() {}
}
