/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2004-2013, 2015, 2017, 2018, 2019, 2021, 2022  AO Industries, Inc.
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
import com.aoapps.hodgepodge.io.FifoFile;
import com.aoapps.hodgepodge.io.FifoFileInputStream;
import com.aoapps.hodgepodge.io.FifoFileOutputStream;
import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
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

	/** Make no instances. */
	private RandomHandler() {throw new AssertionError();}

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

	public static long addMasterEntropy(
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
			return fifo.getOutputStream().available();
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

		FifoFile fifo = getFifoFile();
		synchronized(fifo) {
			return fifo.getOutputStream().available();
		}
	}
}
