/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2009-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master;

import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.hodgepodge.logging.QueuedHandler;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.reseller.Category;
import com.aoindustries.aoserv.client.ticket.Language;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.TicketType;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * The same as <code>com.aoindustries.aoserv.client.TicketLoggingHandler</code>
 * except with direct database access.
 * 
 * @author  AO Industries, Inc.
 */
public class TicketLoggingHandler extends QueuedHandler {

	private final String summaryPrefix;
	private final int category;

	/**
	 * Public constructor required so can be specified in <code>logging.properties</code>.
	 */
	public TicketLoggingHandler() {
		this("AOServ Master", Category.AOSERV_MASTER_PKEY);
	}

	public TicketLoggingHandler(String summaryPrefix, int category) {
		super("Ticket logger for " + summaryPrefix);
		this.summaryPrefix = summaryPrefix;
		this.category = category;
	}

	@Override
	protected void backgroundPublish(Formatter formatter, LogRecord record, String fullReport) throws IOException, SQLException {
		Account.Name rootAccounting = AccountHandler.getRootAccount();
		Level level = record.getLevel();
		// Generate the summary from level, prefix classname, method
		StringBuilder tempSB = new StringBuilder();
		tempSB.append('[').append(level).append(']');
		if(summaryPrefix != null && summaryPrefix.length() > 0) tempSB.append(' ').append(summaryPrefix);
		tempSB.append(" - ").append(record.getSourceClassName()).append(" - ").append(record.getSourceMethodName());
		String summary = tempSB.toString();

		// Start the transaction
		try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
			InvalidateList invalidateList = new InvalidateList();
			// Look for an existing ticket to append
			int existingTicket = conn.queryInt(
				"select\n"
				+ "  coalesce(\n"
				+ "    (\n"
				+ "      select\n"
				+ "        id\n"
				+ "      from\n"
				+ "        ticket.\"Ticket\"\n"
				+ "      where\n"
				+ "        status in (?,?,?)\n"
				+ "        and brand=?\n"
				+ "        and accounting=?\n"
				+ "        and language=?\n"
				+ "        and ticket_type=?\n"
				+ "        and summary=?\n"
				+ "        and category=?\n"
				+ "      order by\n"
				+ "        open_date desc,\n"
				+ "        id desc\n"
				+ "      limit 1\n"
				+ "    ), -1\n"
				+ "  )",
				Status.OPEN,
				Status.HOLD,
				Status.BOUNCED,
				rootAccounting,
				rootAccounting,
				Language.EN,
				TicketType.LOGS,
				summary,
				category
			);
			if(existingTicket != -1) {
				TicketHandler.addTicketAnnotation(
					conn,
					invalidateList,
					existingTicket,
					null,
					com.aoindustries.aoserv.client.ticket.TicketLoggingHandler.generateActionSummary(formatter, record),
					fullReport
				);
			} else {
				// The priority depends on the log level
				String priorityName = com.aoindustries.aoserv.client.ticket.TicketLoggingHandler.getPriorityName(level);
				TicketHandler.addTicket(
					conn,
					invalidateList,
					rootAccounting,
					rootAccounting,
					rootAccounting,
					Language.EN,
					null,
					category,
					TicketType.LOGS,
					null,
					summary,
					fullReport,
					null,
					priorityName,
					null,
					Status.OPEN,
					-1,
					Collections.emptySet(),
					"",
					""
				);
				conn.commit();
			}
			MasterServer.invalidateTables(conn, invalidateList, null);
		}
	}
}
