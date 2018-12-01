/*
 * Copyright 2004-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.AOServConnector;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.master.UserHost;
import com.aoindustries.aoserv.client.validator.UserId;
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
			if(fifoFile==null) fifoFile=new FifoFile(MasterConfiguration.getEntropyPoolFilePath(), AOServConnector.MASTER_ENTROPY_POOL_SIZE);
			return fifoFile;
		}
	}

	private static void checkAccessEntropy(DatabaseConnection conn, RequestSource source, String action) throws IOException, SQLException {
		boolean isAllowed=false;

		UserId mustring=source.getUsername();
		User mu = MasterServer.getUser(conn, mustring);
		if (mu!=null) {
			UserHost[] masterServers=MasterServer.getUserHosts(conn, mustring);
			if(masterServers.length==0) isAllowed=true;
			else {
				for (UserHost masterServer : masterServers) {
					if (ServerHandler.isAOServer(conn, masterServer.getServerPKey())) {
						isAllowed=true;
						break;
					}
				}
			}
		}
		if(!isAllowed) {
			String message=
				"business_administrator.username="
				+mustring
				+" is not allowed to access the master entropy pool: action='"
				+action
			;
			throw new SQLException(message);
		}
	}

	public static void addMasterEntropy(
		DatabaseConnection conn,
		RequestSource source,
		byte[] entropy
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "addMasterEntropy");

		FifoFile file=getFifoFile();
		synchronized(file) {
			FifoFileOutputStream fileOut=file.getOutputStream();
			long available=fileOut.available();
			int addCount=entropy.length;
			if(available<addCount) addCount=(int)available;
			fileOut.write(entropy, 0, addCount);
		}
	}

	public static byte[] getMasterEntropy(
		DatabaseConnection conn,
		RequestSource source,
		int numBytes
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "getMasterEntropy");

		FifoFile file=getFifoFile();
		synchronized(file) {
			FifoFileInputStream fileIn=file.getInputStream();
			long available=fileIn.available();
			if(available<numBytes) numBytes=(int)available;
			byte[] buff=new byte[numBytes];
			int pos=0;
			while(pos<numBytes) {
				int ret=fileIn.read(buff, pos, numBytes-pos);
				if(ret==-1) throw new EOFException("Unexpected EOF");
				pos+=ret;
			}
			return buff;
		}
	}

	public static long getMasterEntropyNeeded(
		DatabaseConnection conn,
		RequestSource source
	) throws IOException, SQLException {
		checkAccessEntropy(conn, source, "getMasterEntropyNeeded");

		FifoFile file=getFifoFile();
		synchronized(file) {
			return file.getMaximumFifoLength()-file.getLength();
		}
	}

	private RandomHandler() {}
}
