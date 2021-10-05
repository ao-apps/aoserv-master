/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2001-2013, 2015, 2017, 2018, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.cron.CronDaemon;
import com.aoapps.cron.CronJob;
import com.aoapps.cron.Schedule;
import com.aoapps.dbc.DatabaseAccess;
import com.aoapps.dbc.DatabaseConnection;
import com.aoapps.lang.Strings;
import com.aoapps.net.Email;
import com.aoindustries.aoserv.client.AOServObject;
import com.aoindustries.aoserv.client.account.Account;
import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.ticket.ActionType;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.TicketType;
import java.io.IOException;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The <code>TicketHandler</code> handles all the accesses to the ticket tables.
 *
 * @author  AO Industries, Inc.
 */
public final class TicketHandler /*implements Runnable*/ {

	private static final Logger logger = Logger.getLogger(TicketHandler.class.getName());

	private TicketHandler() {
	}

	// <editor-fold desc="Security">
	/**
	 * To be able to access a ticket action, must both have access to its
	 */
	public static boolean canAccessTicketAction(DatabaseConnection conn, RequestSource source, int action) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length==0) return true; // Master users
			else return false; // daemons
		} else {
			Account.Name account = getAccountForAction(conn, action);
			if(isTicketAdmin(conn, source)) {
				// Must have access to either the account or reseller
				if(account!=null && AccountHandler.canAccessAccount(conn, source, account)) return true;
				Account.Name reseller = getResellerForAction(conn, action);
				return AccountHandler.canAccessAccount(conn, source, reseller);
			} else {
				// Must have access to the business and may not be an admin-only action
				if(account==null || !AccountHandler.canAccessAccount(conn, source, account)) return false;
				return !getVisibleAdminOnlyForAction(conn, action);
			}
		}
	}

	public static void checkAccessAction(DatabaseConnection conn, RequestSource source, String verb, int action) throws IOException, SQLException {
		if(!canAccessTicketAction(conn, source, action)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access ticket_action: verb='"
				+verb
				+", action="
				+action
			;
			throw new SQLException(message);
		}
	}

	public static boolean canAccessTicket(DatabaseConnection conn, RequestSource source, int ticket) throws IOException, SQLException {
		User mu = MasterServer.getUser(conn, source.getCurrentAdministrator());
		if(mu!=null) {
			if(MasterServer.getUserHosts(conn, source.getCurrentAdministrator()).length==0) {
				// Master users
				return true;
			} else {
				// daemons

				// Can only access their own logs ticket.Ticket
				Account.Name currentAdministrator_account = AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator());
				String status = getStatusForTicket(conn, ticket);
				return
					currentAdministrator_account.equals(getBrandForTicket(conn, ticket))
					&& currentAdministrator_account.equals(getAccountForTicket(conn, ticket))
					&& (
						Status.OPEN.equals(status)
						|| Status.HOLD.equals(status)
						|| Status.BOUNCED.equals(status)
					)
					&& TicketType.LOGS.equals(getTypeForTicket(conn, ticket))
				;
			}
		} else {
			Account.Name account = getAccountForTicket(conn, ticket);
			if(isTicketAdmin(conn, source)) {
				// Must have access to either the account or reseller
				if(account!=null && AccountHandler.canAccessAccount(conn, source, account)) return true;
				Account.Name reseller = getResellerForTicket(conn, ticket);
				return AccountHandler.canAccessAccount(conn, source, reseller);
			} else {
				// Must have access to the business
				return account!=null && AccountHandler.canAccessAccount(conn, source, account);
			}
		}
	}

	public static void checkAccessTicket(DatabaseConnection conn, RequestSource source, String action, int ticket) throws IOException, SQLException {
		if(!canAccessTicket(conn, source, ticket)) {
			String message=
				"currentAdministrator="
				+source.getCurrentAdministrator()
				+" is not allowed to access ticket: action='"
				+action
				+", ticket="
				+ticket
			;
			throw new SQLException(message);
		}
	}
	// </editor-fold>
	// <editor-fold desc="Add Ticket">
	/**
	 * Generates a random, unused ticket ID.
	 */
	public static int generateTicketId(
		DatabaseConnection conn
	) throws IOException, SQLException {
		SecureRandom secureRandom = MasterServer.getSecureRandom();
		for(int range=1000000; range<1000000000; range *= 10) {
			for(int attempt=0; attempt<1000; attempt++) {
				int ticket = secureRandom.nextInt(range);
				if(conn.queryBoolean("select (select id from ticket.\"Ticket\" where id=?) is null", ticket)) return ticket;
			}
		}
		throw new SQLException("Failed to generate ticket ID after thousands of attempts");
	}

	/**
	 * Adds a ticket with security checks.
	 */
	public static int addTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		Account.Name brand,
		Account.Name account,
		String language,
		int category,
		String type,
		Email fromAddress,
		String summary,
		String details,
		String clientPriority,
		Set<Email> contactEmails,
		String contactPhoneNumbers
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "addTicket", Permission.Name.add_ticket);
		boolean isAdmin=isTicketAdmin(conn, source);
		if(account==null) {
			// if(!isAdmin) throw new SQLException("Only ticket administrators may create a ticket without a business.");
		} else {
			AccountHandler.checkAccessAccount(conn, source, "addTicket", account);
			if(AccountHandler.isAccountDisabled(conn, account)) throw new SQLException("Unable to add Ticket, Account disabled: "+account);
		}
		Account.Name reseller = ResellerHandler.getResellerForAccountAutoEscalate(
			conn,
			account==null ? AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator()) : account
		);
		return addTicket(
			conn,
			invalidateList,
			brand,
			reseller,
			account,
			language,
			source.getCurrentAdministrator(),
			category,
			type,
			fromAddress,
			summary,
			details,
			null, // raw_email
			clientPriority,
			null, // admin_priority
			Status.OPEN,
			-1,
			contactEmails,
			contactPhoneNumbers,
			""
		);
	}

	/**
	 * Adds a ticket directly, without any security checks.
	 */
	public static int addTicket(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		Account.Name brand,
		Account.Name reseller,
		Account.Name account,
		String language,
		com.aoindustries.aoserv.client.account.User.Name createdBy,
		int category,
		String type,
		Email fromAddress,
		String summary,
		String details,
		String rawEmail,
		String clientPriority,
		String adminPriority,
		String status,
		long statusTimeout,
		Set<Email> contactEmails,
		String contactPhoneNumbers,
		String internalNotes
	) throws IOException, SQLException {
		int ticket = generateTicketId(conn);

		conn.update(
			AOServObject.USE_SQL_DATA_WRITE
				? "insert into ticket.\"Ticket\" values(?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,?,?,?,?,?)"
				: "insert into ticket.\"Ticket\" values(?,?,?,?,?,?,?,?,?::\"com.aoapps.net\".\"Email\",?,?,?,now(),?,?,?,?,?,?,?)",
			ticket,
			brand,
			reseller,
			account,
			language,
			createdBy,
			category==-1 ? DatabaseAccess.Null.INTEGER : category,
			type,
			AOServObject.USE_SQL_DATA_WRITE ? fromAddress : Objects.toString(fromAddress, null),
			summary,
			details,
			rawEmail,
			clientPriority,
			adminPriority,
			status,
			statusTimeout==-1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(statusTimeout),
			// TODO: Array
			Strings.join(contactEmails, ", "),
			contactPhoneNumbers,
			internalNotes
		);

		/*
		if(
			clientPriority.equals(Priority.URGENT)
			&& !account.equals(AccountHandler.getRootAccount()))
		{
			String message =
				"Account:   "+account+"\n"+
				"Username:   "+username+"\n"+
				"Type:       "+type+"\n"+
				"Deadline:   "+(deadline==-1?"":SQLUtility.formatDate(deadline, timeZone))+"\n"+
				"Technology: "+(technology==null?"":technology)+"\n"+
				"\n\n"+
				details+"\n\n"+
				MasterConfiguration.getTicketURL()+id
			;
			sendEmail("support@aoindustries.com", "URGENT ticket notification", message, "ID: "+id+" "+details);
		}*/

		// Notify all clients of the updates
		if(account!=null) invalidateList.addTable(conn, Table.TableID.TICKETS, account, InvalidateList.allHosts, false);
		invalidateList.addTable(conn, Table.TableID.TICKETS, brand, InvalidateList.allHosts, false);
		invalidateList.addTable(conn, Table.TableID.TICKETS, reseller, InvalidateList.allHosts, false);
		//invalidateList.addTable(conn, Table.TableID.ACTIONS, account, null);
		return ticket;
	}
	// </editor-fold>
	// <editor-fold desc="Delayed Data Access">
	public static String getTicketDetails(
		DatabaseConnection conn,
		RequestSource source,
		int ticket
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "getTicketDetails", ticket);
		return conn.queryString("select details from ticket.\"Ticket\" where id=?", ticket);
	}

	public static String getTicketRawEmail(
		DatabaseConnection conn,
		RequestSource source,
		int ticket
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "getTicketRawEmail", ticket);
		return conn.queryString("select raw_email from ticket.\"Ticket\" where id=?", ticket);
	}

	public static String getTicketInternalNotes(
		DatabaseConnection conn,
		RequestSource source,
		int ticket
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "getTicketInternalNotes", ticket);
		if(isTicketAdmin(conn, source)) return conn.queryString("select internal_notes from ticket.\"Ticket\" where id=?", ticket);
		else return "";
	}

	public static String getActionOldValue(
		DatabaseConnection conn,
		RequestSource source,
		int action
	) throws IOException, SQLException {
		checkAccessAction(conn, source, "getActionOldValue", action);
		return conn.queryString("select old_value from ticket.\"Action\" where id=?", action);
	}

	public static String getActionNewValue(
		DatabaseConnection conn,
		RequestSource source,
		int action
	) throws IOException, SQLException {
		checkAccessAction(conn, source, "getActionNewValue", action);
		return conn.queryString("select new_value from ticket.\"Action\" where id=?", action);
	}

	public static String getActionDetails(
		DatabaseConnection conn,
		RequestSource source,
		int action
	) throws IOException, SQLException {
		checkAccessAction(conn, source, "getActionDetails", action);
		return conn.queryString("select details from ticket.\"Action\" where id=?", action);
	}

	public static String getActionRawEmail(
		DatabaseConnection conn,
		RequestSource source,
		int action
	) throws IOException, SQLException {
		checkAccessAction(conn, source, "getActionRawEmail", action);
		return conn.queryString("select raw_email from ticket.\"Action\" where id=?", action);
	}
	// </editor-fold>
	// <editor-fold desc="Ticket Actions">
	/*

	public static void bounceTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "bounceTicket", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "bounceTicket", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to bounce a Ticket as user '"+User.MAIL+'\'');

		conn.update("update ticket.\"Ticket\" set status=? where id=?", Status.BOUNCED, ticket);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			ActionType.BOUNCED,
			comments
		);

		// Notify all clients of the update
		String account = getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);

		if(account!=null && !account.equals(AccountHandler.getRootAccount())) {
			String to = getContactEmails(conn, ticket);
			String message =
				"AO Industries support has returned Ticket "+ticket+" to you.\n"+
				"More information is required to complete this ticket.  Please\n"+
				"browse to \n\n"+
				MasterConfiguration.getTicketURL()+ticket+"\n\n"+
				"for more details.\n\n"+
				"--------------------------------------------------------------\n"+
				"This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
			;
			sendEmail(to, "Ticket "+ticket+" was bounced.", message, null);
		}
	}

	public static void changeTicketAdminPriority(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String priority,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "changeTicketAdminPriority", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "changeTicketAdminPriority", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to change a Ticket's admin priority as user '"+User.MAIL+'\'');

		if(!isTicketAdmin(conn, source)) throw new SQLException("Only ticket administrators may change the administrative priority.");
		String oldValue=conn.queryString("select admin_priority from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set admin_priority=? where id=?", priority, ticket);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
			ticket,
			username,
			ActionType.ADMIN_PRIORITY_CHANGE,
			oldValue,
			comments
		);

		// Notify all clients of the update
		String account = getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);
	}

	public static void setTicketAssignedTo(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String assignedTo,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "setTicketAssignedTo", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "setTicketAssignedTo", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to assign a Ticket as user '"+User.MAIL+'\'');

		if(!isTicketAdmin(conn, source)) throw new SQLException("Only ticket administrators may assign ticket.Ticket.");
		String oldValue=conn.queryString("select assigned_to from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set assigned_to=? where id=?", assignedTo, ticket);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
			ticket,
			username,
			ActionType.ASSIGNED,
			oldValue,
			comments
		);

		// Notify all clients of the update
		String account = getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);
	}
*/
	public static boolean setTicketAccount(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		Account.Name oldAccount,
		Account.Name newAccount
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketBusiness", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketBusiness", ticket);

		if(newAccount!=null) AccountHandler.checkAccessAccount(conn, source, "setTicketBusiness", newAccount);

		int updateCount;
		if(oldAccount==null) updateCount = conn.update("update ticket.\"Ticket\" set accounting=? where id=? and accounting is null", newAccount, ticket);
		else updateCount = conn.update("update ticket.\"Ticket\" set accounting=? where id=? and accounting=?", newAccount, ticket, oldAccount);

		if(updateCount==1) {
			conn.update(
				"insert into ticket.\"Action\"(ticket, administrator, action_type, old_accounting, new_accounting) values(?,?,?,?,?)",
				ticket,
				source.getCurrentAdministrator(),
				ActionType.SET_BUSINESS,
				oldAccount,
				newAccount
			);

			// Notify all clients of the update
			// By oldAccount
			if(oldAccount!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					oldAccount,
					InvalidateList.allHosts,
					false
				);
				invalidateList.addTable(conn,
					Table.TableID.TICKET_ACTIONS,
					oldAccount,
					InvalidateList.allHosts,
					false
				);
			}
			// By newAccount
			if(newAccount!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					newAccount,
					InvalidateList.allHosts,
					false
				);
				invalidateList.addTable(conn,
					Table.TableID.TICKET_ACTIONS,
					newAccount,
					InvalidateList.allHosts,
					false
				);
			}
			// By brand
			Account.Name brand = conn.queryObject(
				ObjectFactories.accountNameFactory,
				"select brand from ticket.\"Ticket\" where id=?",
				ticket
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				brand,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				brand,
				InvalidateList.allHosts,
				false
			);
			// By reseller
			Account.Name reseller = conn.queryObject(
				ObjectFactories.accountNameFactory,
				"select reseller from ticket.\"Ticket\" where id=?",
				ticket
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			return true;
		} else if(updateCount==0) {
			return false;
		} else {
			throw new SQLException("Unexpected update count: "+updateCount);
		}
	}

	public static boolean setTicketType(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String oldType,
		String newType
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketType", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketType", ticket);

		int updateCount = conn.update(
			"update ticket.\"Ticket\" set ticket_type=? where id=? and ticket_type=?",
			newType,
			ticket,
			oldType
		);
		if(updateCount==1) {
			conn.update(
				"insert into ticket.\"Action\"(ticket, administrator, action_type, old_type, new_type) values(?,?,?,?,?)",
				ticket,
				source.getCurrentAdministrator(),
				ActionType.SET_TYPE,
				oldType,
				newType
			);

			// Notify all clients of the update
			// By account
			Account.Name account = getAccountForTicket(conn, ticket);
			if(account!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					account,
					InvalidateList.allHosts,
					false
				);
				invalidateList.addTable(conn,
					Table.TableID.TICKET_ACTIONS,
					account,
					InvalidateList.allHosts,
					false
				);
			}
			// By brand
			Account.Name brand = getBrandForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				brand,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				brand,
				InvalidateList.allHosts,
				false
			);
			// By reseller
			Account.Name reseller = getResellerForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			return true;
		} else if(updateCount==0) {
			return false;
		} else {
			throw new SQLException("Unexpected update count: "+updateCount);
		}
	}

	public static boolean setTicketStatus(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String oldStatus,
		String newStatus,
		long statusTimeout
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketStatus", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketStatus", ticket);

		int updateCount = conn.update(
			"update ticket.\"Ticket\" set status=?, status_timeout=? where id=? and status=?",
			newStatus,
			statusTimeout==-1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(statusTimeout),
			ticket,
			oldStatus
		);
		if(updateCount==1) {
			conn.update(
				"insert into ticket.\"Action\"(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
				ticket,
				source.getCurrentAdministrator(),
				ActionType.SET_STATUS,
				oldStatus,
				newStatus
			);

			// Notify all clients of the update
			// By account
			Account.Name account = getAccountForTicket(conn, ticket);
			if(account!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					account,
					InvalidateList.allHosts,
					false
				);
				invalidateList.addTable(conn,
					Table.TableID.TICKET_ACTIONS,
					account,
					InvalidateList.allHosts,
					false
				);
			}
			// By brand
			Account.Name brand = getBrandForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				brand,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				brand,
				InvalidateList.allHosts,
				false
			);
			// By reseller
			Account.Name reseller = getResellerForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			return true;
		} else if(updateCount==0) {
			return false;
		} else {
			throw new SQLException("Unexpected update count: "+updateCount);
		}
	}

	public static boolean setTicketInternalNotes(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String oldInternalNotes,
		String newInternalNotes
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketInternalNotes", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketInternalNotes", ticket);

		int updateCount = conn.update(
			"update ticket.\"Ticket\" set internal_notes=? where id=? and internal_notes=?",
			newInternalNotes,
			ticket,
			oldInternalNotes
		);
		if(updateCount==1) {
			conn.update(
				"insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
				ticket,
				source.getCurrentAdministrator(),
				ActionType.SET_INTERNAL_NOTES,
				oldInternalNotes,
				newInternalNotes
			);

			// Notify all clients of the update
			// By account
			Account.Name account = getAccountForTicket(conn, ticket);
			if(account!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					account,
					InvalidateList.allHosts,
					false
				);
				invalidateList.addTable(conn,
					Table.TableID.TICKET_ACTIONS,
					account,
					InvalidateList.allHosts,
					false
				);
			}
			// By brand
			Account.Name brand = getBrandForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				brand,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				brand,
				InvalidateList.allHosts,
				false
			);
			// By reseller
			Account.Name reseller = getResellerForTicket(conn, ticket);
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				reseller,
				InvalidateList.allHosts,
				false
			);
			return true;
		} else if(updateCount==0) {
			return false;
		} else {
			throw new SQLException("Unexpected update count: "+updateCount);
		}
	}

	public static void setTicketContactEmails(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		// TODO: oldContactEmails as a concurrent update check
		Set<Email> contactEmails
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketContactEmails", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketContactEmails", ticket);

		String oldValue = conn.queryString("select contact_emails from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set contact_emails=? where id=?", contactEmails, ticket);

		conn.update(
			"insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
			ticket,
			source.getCurrentAdministrator(),
			ActionType.SET_CONTACT_EMAILS,
			oldValue,
			// TODO: Array
			Strings.join(contactEmails, ", ")
		);

		// Notify all clients of the update
		// By account
		Account.Name account = getAccountForTicket(conn, ticket);
		if(account!=null) {
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				account,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				account,
				InvalidateList.allHosts,
				false
			);
		}
		// By brand
		Account.Name brand = getBrandForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			brand,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			brand,
			InvalidateList.allHosts,
			false
		);
		// By reseller
		Account.Name reseller = getResellerForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			reseller,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			reseller,
			InvalidateList.allHosts,
			false
		);
	}

	public static void setTicketContactPhoneNumbers(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		// TODO: oldContactPhoneNumbers as a concurrent update check
		String contactPhoneNumbers
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketContactPhoneNumbers", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketContactPhoneNumbers", ticket);

		String oldValue = conn.queryString("select contact_phone_numbers from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set contact_phone_numbers=? where id=?", contactPhoneNumbers, ticket);

		conn.update(
			"insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
			ticket,
			source.getCurrentAdministrator(),
			ActionType.SET_CONTACT_PHONE_NUMBERS,
			oldValue,
			contactPhoneNumbers
		);

		// Notify all clients of the update
		// By account
		Account.Name account = getAccountForTicket(conn, ticket);
		if(account!=null) {
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				account,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				account,
				InvalidateList.allHosts,
				false
			);
		}
		// By brand
		Account.Name brand = getBrandForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			brand,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			brand,
			InvalidateList.allHosts,
			false
		);
		// By reseller
		Account.Name reseller = getResellerForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			reseller,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			reseller,
			InvalidateList.allHosts,
			false
		);
	}

	public static void changeTicketClientPriority(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		// TODO: oldClientPriority as a concurrent update check
		String newClientPriority
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "changeTicketClientPriority", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "changeTicketClientPriority", ticket);

		String oldClientPriority = conn.queryString("select client_priority from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set client_priority=? where id=?", newClientPriority, ticket);

		conn.update(
			"insert into ticket.\"Action\"(ticket, administrator, action_type, old_priority, new_priority) values(?,?,?,?,?)",
			ticket,
			source.getCurrentAdministrator(),
			ActionType.SET_CLIENT_PRIORITY,
			oldClientPriority,
			newClientPriority
		);

		// Notify all clients of the update
		// By account
		Account.Name account = getAccountForTicket(conn, ticket);
		if(account!=null) {
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				account,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				account,
				InvalidateList.allHosts,
				false
			);
		}
		// By brand
		Account.Name brand = getBrandForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			brand,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			brand,
			InvalidateList.allHosts,
			false
		);
		// By reseller
		Account.Name reseller = getResellerForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			reseller,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			reseller,
			InvalidateList.allHosts,
			false
		);
	}

	public static void setTicketSummary(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		// TODO: oldSummary as a concurrent update check
		String summary
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "setTicketSummary", Permission.Name.edit_ticket);
		checkAccessTicket(conn, source, "setTicketSummary", ticket);

		String oldValue = conn.queryString("select summary from ticket.\"Ticket\" where id=?", ticket);

		conn.update("update ticket.\"Ticket\" set summary=? where id=?", summary, ticket);

		conn.update(
			"insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
			ticket,
			source.getCurrentAdministrator(),
			ActionType.SET_SUMMARY,
			oldValue,
			summary
		);

		// Notify all clients of the update
		// By account
		Account.Name account = getAccountForTicket(conn, ticket);
		if(account!=null) {
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				account,
				InvalidateList.allHosts,
				false
			);
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				account,
				InvalidateList.allHosts,
				false
			);
		}
		// By brand
		Account.Name brand = getBrandForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			brand,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			brand,
			InvalidateList.allHosts,
			false
		);
		// By reseller
		Account.Name reseller = getResellerForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKETS,
			reseller,
			InvalidateList.allHosts,
			false
		);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			reseller,
			InvalidateList.allHosts,
			false
		);
	}

	/**
	 * Adds an annotation with security checks.
	 */
	public static void addTicketAnnotation(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String summary,
		String details
	) throws IOException, SQLException {
		AccountHandler.checkPermission(conn, source, "addTicketAnnotation", Permission.Name.add_ticket);
		checkAccessTicket(conn, source, "addTicketAnnotation", ticket);

		addTicketAnnotation(conn, invalidateList, ticket, source.getCurrentAdministrator(), summary, details);
	}

	/**
	 * Adds an annotation <b>without</b> security checks.
	 */
	public static void addTicketAnnotation(
		DatabaseConnection conn,
		InvalidateList invalidateList,
		int ticket,
		com.aoindustries.aoserv.client.account.User.Name administrator,
		String summary,
		String details
	) throws IOException, SQLException {
		conn.update(
			"insert into ticket.\"Action\"(ticket, administrator, action_type, summary, details) values(?,?,?,?,?)",
			ticket,
			administrator,
			ActionType.ADD_ANNOTATION,
			summary,
			details
		);
		// By account
		Account.Name account = getAccountForTicket(conn, ticket);
		if(account!=null) {
			invalidateList.addTable(conn,
				Table.TableID.TICKET_ACTIONS,
				account,
				InvalidateList.allHosts,
				false
			);
		}
		// By brand
		Account.Name brand = getBrandForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			brand,
			InvalidateList.allHosts,
			false
		);
		// By reseller
		Account.Name reseller = getResellerForTicket(conn, ticket);
		invalidateList.addTable(conn,
			Table.TableID.TICKET_ACTIONS,
			reseller,
			InvalidateList.allHosts,
			false
		);

		// Reopen if needed
		String status = conn.queryString("select status from ticket.\"Ticket\" where id=?", ticket);
		if(
			status.equals(Status.BOUNCED)
			|| status.equals(Status.CLOSED)
		) {
			conn.update("update ticket.\"Ticket\" set status=?, status_timeout=null where id=?", Status.OPEN, ticket);
			conn.update(
				"insert into ticket.\"Action\"(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
				ticket,
				administrator,
				ActionType.SET_STATUS,
				status,
				Status.OPEN
			);
			// By account
			if(account!=null) {
				invalidateList.addTable(conn,
					Table.TableID.TICKETS,
					account,
					InvalidateList.allHosts,
					false
				);
			}
			// By brand
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				brand,
				InvalidateList.allHosts,
				false
			);
			// By reseller
			invalidateList.addTable(conn,
				Table.TableID.TICKETS,
				reseller,
				InvalidateList.allHosts,
				false
			);
		}
	}
/*
	public static void completeTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "completeTicket", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "completeTicket", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to complete Ticket as user '"+User.MAIL+'\'');

		conn.update(
			"update ticket.\"Ticket\" set close_date=now(), closed_by=?, status=?, assigned_to=null where id=?",
			username,
			Status.COMPLETED,
			ticket
		);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			ActionType.COMPLETE_TICKET,
			comments
		);

		// Notify all clients of the update
		String account = getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);

		if(account!=null && !account.equals(AccountHandler.getRootAccount())) {
			String to = getContactEmails(conn, ticket);
			String message =
				"AO Industries support has completed Ticket "+ticket+".\n"+
				"Please browse to \n\n"+
				MasterConfiguration.getTicketURL()+ticket+"\n\n"+
				"for more details.\n\n"+
				"--------------------------------------------------------------\n"+
				"This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
			;
			sendEmail(to, "Ticket "+ticket+" was completed.", message, null);
		}
	}

	public static void holdTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "holdTicket", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		String username=source.getUsername();
		UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);

		String baAccount = UsernameHandler.getAccountForUsername(conn, username);
		boolean isAdminChange=baAccount.equals(AccountHandler.getRootAccount());

		conn.update(
			"update ticket.\"Ticket\" set status=? where id=?",
			isAdminChange?Status.ADMIN_HOLD:Status.CLIENT_HOLD,
			ticket
		);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			isAdminChange?ActionType.ADMIN_HOLD:ActionType.CLIENT_HOLD,
			comments
		);

		// Notify all clients of the update
		String account=getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);
	}

	public static void killTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "holdTicket", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to kill Ticket as user '"+User.MAIL+'\'');

		String account1 = conn.queryString("select pk.accounting from account.\"User\" un, billing.\"Package\" pk where un.username=? and un.package=pk.name", username);
		String account2 = conn.queryString("select accounting from ticket.\"Ticket\" where id=?", ticket);

		boolean isClientChange=account1.equals(account2);

		conn.update(
			"update ticket.\"Ticket\" set close_date=now(), closed_by=?, status=?, assigned_to=null where id=?",
			username,
			isClientChange?Status.CLIENT_KILL:Status.ADMIN_KILL,
			ticket
		);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			isClientChange?ActionType.CLIENT_KILLED:ActionType.ADMIN_KILL,
			comments
		);

		// Notify all clients of the update
		String account=getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);
*/
		/*
		if(account!=null && !account.equals(AccountHandler.getRootAccount())) {
			String to = getContactEmails(conn, ticket);
			String message =
				"AO Industries support has killed Ticket "+ticket+".\n"+
				"Please browse to \n\n"+
				MasterConfiguration.getTicketURL()+ticket+"\n\n"+
				"for more details.\n\n"+
				"--------------------------------------------------------------\n"+
				"This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
			;
			sendEmail(to, "Ticket "+ticket+" was killed.", message, null);
		}*/
	//}

	/*
	public static void reactivateTicket(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "holdTicket", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to reactivate Ticket as user '"+User.MAIL+'\'');

		conn.update("update ticket.\"Ticket\" set status=? where id=?", Status.UNDERWAY, ticket);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			ActionType.REACTIVATE_TICKET,
			comments
		);

		// Notify all clients of the update
		String account=getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);

		if(account!=null && !account.equals(AccountHandler.getRootAccount())) {
			String to = getContactEmails(conn, ticket);
			String message =
				"AO Industries support has reactivated Ticket "+ticket+".\n"+
				"Please browse to \n\n"+
				MasterConfiguration.getTicketURL()+ticket+"\n\n"+
				"for more details.\n\n"+
				"--------------------------------------------------------------\n"+
				"This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
			;
			sendEmail(to, "Ticket "+ticket+" was reactivated.", message, null);
		}
	}

	public static void ticketWork(
		DatabaseConnection conn,
		RequestSource source,
		InvalidateList invalidateList,
		int ticket,
		String username,
		String comments
	) throws IOException, SQLException {
		checkAccessTicket(conn, source, "ticketWork", ticket);
		if(!isActive(conn, ticket)) throw new SQLException("Ticket no longer active: "+ticket);
		UsernameHandler.checkAccessUsername(conn, source, "ticketWork", username);
		if(username.equals(User.MAIL)) throw new SQLException("Not allowed to submit Ticket work as user '"+User.MAIL+'\'');

		conn.update("update ticket.\"Ticket\" set status=? where id=?", Status.UNDERWAY, ticket);

		conn.update(
			"insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
			ticket,
			username,
			ActionType.WORK_ENTRY,
			comments
		);

		// Notify all clients of the update
		String account=getAccountForTicket(conn, ticket);
		invalidateList.addTable(
			conn,
			Table.TableID.TICKETS,
			account,
			InvalidateList.allServers,
			false
		);
		invalidateList.addTable(
			conn,
			Table.TableID.ACTIONS,
			account,
			InvalidateList.allServers,
			false
		);

		if(account!=null && !account.equals(AccountHandler.getRootAccount())) {
			String to = getContactEmails(conn, ticket);
			String message =
				"AO Industries support has updated Ticket "+ticket+".\n"+
				"Please browse to \n\n"+
				MasterConfiguration.getTicketURL()+ticket+"\n\n"+
				"for more details.\n\n"+
				"--------------------------------------------------------------\n"+
				"This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
			;
			sendEmail(to, "Ticket "+ticket+" was updated.", message, null);
		}
	}*/
	// </editor-fold>
	// <editor-fold desc="Internal Master Data Access">
	/**
	 * A ticket administrator is part of a business that is also a reseller.
	 */
	public static boolean isTicketAdmin(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
		return conn.queryBoolean("select (select accounting from reseller.\"Reseller\" where accounting=?) is not null", AccountUserHandler.getAccountForUser(conn, source.getCurrentAdministrator()));
	}

	public static Account.Name getAccountForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select accounting from ticket.\"Ticket\" where id=?",
			ticket
		);
	}

	public static Account.Name getBrandForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select brand from ticket.\"Ticket\" where id=?",
			ticket
		);
	}

	public static String getStatusForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		return conn.queryString("select status from ticket.\"Ticket\" where id=?", ticket);
	}

	public static String getTypeForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		return conn.queryString("select ticket_type from ticket.\"Ticket\" where id=?", ticket);
	}

	public static Account.Name getResellerForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select reseller from ticket.\"Ticket\" where id=?",
			ticket
		);
	}

	public static Account.Name getAccountForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select ti.accounting from ticket.\"Action\" ac, ticket.\"Ticket\" ti where ac.id=? and ac.ticket=ti.id",
			action
		);
	}

	public static Account.Name getResellerForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
		return conn.queryObject(
			ObjectFactories.accountNameFactory,
			"select ti.reseller from ticket.\"Action\" ac, ticket.\"Ticket\" ti where ac.id=? and ac.ticket=ti.id",
			action
		);
	}

	public static boolean getVisibleAdminOnlyForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
		return conn.queryBoolean("select tat.visible_admin_only from ticket.\"Action\" ac inner join ticket.\"ActionType\" tat on ac.action_type=tat.type where ac.id=?", action);
	}

	/*
	public static boolean isActive(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		String status=conn.queryString("select status from ticket.\"Ticket\" where id=?", ticket);
		return
			!Status.ADMIN_KILL.equals(status)
			&& !Status.CLIENT_KILL.equals(status)
			&& !Status.COMPLETED.equals(status)
		;
	}*/
	// </editor-fold>
	// <editor-fold desc="Email Integration">
	/*
	public static void sendEmail(String to, String subject, String message, String cellMessage) {
		if(to!=null) {
			List<String> tos = Strings.splitStringCommaSpace(to);
			int len = tos.size();
			for (int i = 0; i<len; i++) {
				try {
					String[] orAddys=Strings.splitString(tos.get(i), '|');
					if(orAddys.length>0) {
						MailMessage msg=new MailMessage(MasterConfiguration.getTicketSmtpServer());
						msg.from("support@aoindustries.com");
						msg.to(orAddys[MasterServer.getFastRandom().nextInt(orAddys.length)]);
						msg.setSubject(subject);
						PrintStream email=msg.getPrintStream();

						email.print(message);
						msg.sendAndClose();
					}
				} catch (IOException err) {
					UserHost.reportError(err, null);
				}
			}
		}
		if (cellMessage!=null) {
			 try {
				MailMessage msg = new MailMessage(MasterConfiguration.getTicketSmtpServer());
				msg.from("support@aoindustries.com");
				msg.to("2054542556@tmomail.net");
				msg.setSubject("URGENT");
				PrintStream email = msg.getPrintStream();
				email.print(cellMessage);
				msg.sendAndClose();
			} catch (IOException err) {
				UserHost.reportError(err, null);
			}
		}
	}

	public static String getContactEmails(DatabaseConnection conn, int ticket) throws IOException, SQLException {
		String contactEmails=conn.queryString(
			"select contact_emails from ticket.\"Ticket\" where id=?",
			ticket
		);
		if(contactEmails.length()==0) return AccountHandler.getTechnicalEmail(conn, getAccountForTicket(conn, ticket));
		return contactEmails;
	}

	private static Thread thread;
	 */
	private static boolean cronDaemonAdded = false;

	/**
	 * Runs once every four hours
	 */
	private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==25 && (hour&3)==3;

	@SuppressWarnings("UseOfSystemOutOrSystemErr")
	public static void start() {
		synchronized(System.out) {
			if(!cronDaemonAdded) {
				System.out.print("Starting " + TicketHandler.class.getSimpleName() + ": ");
				CronDaemon.addCronJob(
					new CronJob() {
						@Override
						public Schedule getSchedule() {
							return schedule;
						}

						@Override
						public String getName() {
							return "Clean log ticket.Ticket";
						}

						@Override
						public void run(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
							try {
								try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
									InvalidateList invalidateList = new InvalidateList();
									int updateCount = conn.update(
										"delete from\n"
										+ "  ticket.\"Action\"\n"
										+ "where\n"
										+ "  ticket in (\n"
										+ "    select\n"
										+ "      id\n"
										+ "    from\n"
										+ "      ticket.\"Ticket\"\n"
										+ "    where\n"
										+ "      ticket_type=?\n"
										+ "      and open_date<(now()-'7 days'::interval)\n"
										+ "  ) and time<(now()-'7 days'::interval)",
										TicketType.LOGS
									);
									if(updateCount>0) {
										invalidateList.addTable(
											conn,
											Table.TableID.TICKET_ACTIONS,
											InvalidateList.allAccounts,
											InvalidateList.allHosts,
											false
										);
									}
									updateCount = conn.update(
										"delete from\n"
										+ "  ticket.\"Ticket\" t\n"
										+ "where\n"
										+ "  t.ticket_type=?\n"
										+ "  and t.open_date < (now()-'7 days'::interval)\n"
										+ "  and (select ta.id from ticket.\"Action\" ta where t.id = ta.ticket limit 1) is null",
										TicketType.LOGS
									);
									if(updateCount>0) {
										invalidateList.addTable(
											conn,
											Table.TableID.TICKETS,
											InvalidateList.allAccounts,
											InvalidateList.allHosts,
											false
										);
									}
									conn.commit();
									MasterServer.invalidateTables(conn, invalidateList, null);
								}
							} catch(ThreadDeath TD) {
								throw TD;
							} catch(Throwable T) {
								logger.log(Level.SEVERE, null, T);
							}
						}

						@Override
						public int getThreadPriority() {
							return Thread.NORM_PRIORITY-2;
						}
					},
					logger
				);
				cronDaemonAdded = true;
				//thread=new Thread(new TicketHandler());
				//thread.setPriority(Thread.NORM_PRIORITY-1);
				//thread.start();
				System.out.println("Done");
			}
		}
	}

	/**
	 * The amount of time sleeping between IMAP folder scans.
	 */
	/*
	public static final long
		SLEEP_INTERVAL= (long)60*1000,
		TIMER_MAX_TIME=(long)15*60*1000,
		TIMER_REMINDER_INTERVAL=(long)60*60*1000
	;*/

	/**
	 * The email part separator.
	 */
	//public static final String PART_SEPARATOR="\n----------------------------------------------------------------------------------------------------\n\n";

	/*
	public void run() {
		while(true) {
			try {
				while(true) {
					try {
						Thread.sleep(SLEEP_INTERVAL);
					} catch(InterruptedException err) {
						logger.log(Level.WARNING, null, err);
					}
					ProcessTimer timer=new ProcessTimer(
						MasterConfiguration.getWarningSmtpServer(),
						MasterConfiguration.getWarningEmailFrom(),
						MasterConfiguration.getWarningEmailTo(),
						"Ticket Handler",
						"Creating ticket.Ticket from IMAP folders",
						TIMER_MAX_TIME,
						TIMER_REMINDER_INTERVAL
					);
					try {
						timer.start();
						// Start the transaction
						try (DatabaseConnection conn = MasterDatabase.getDatabase().connect()) {
							InvalidateList invalidateList = new InvalidateList();
							for(int c=1;c<Integer.MAX_VALUE;c++) {
								String hostname=MasterConfiguration.getTicketSource("imap", c, "hostname");
								if(hostname==null) break;
								String username=MasterConfiguration.getTicketSource("imap", c, "username");
								String password=MasterConfiguration.getTicketSource("imap", c, "password");
								String folderName=MasterConfiguration.getTicketSource("imap", c, "folder");
								String archiveFolderName=MasterConfiguration.getTicketSource("imap", c, "archivefolder");
								List<String> ignore_recipients=Strings.splitStringCommaSpace(MasterConfiguration.getTicketSource("imap", c, "ignore_recipients"));
								String assign_to=MasterConfiguration.getTicketSource("imap", c, "assign_to");
								if(assign_to!=null) assign_to=assign_to.trim();

								Properties props = new Properties();
								props.put("mail.store.protocol", "imap");
								Session session=Session.getInstance(props, null);
								Store store=session.getStore();
								try {
									store.connect(
										hostname,
										username,
										password
									);
									Folder folder=store.getFolder(folderName);
									if(!folder.exists()) {
										UserHost.reportWarning(new IOException("Folder does not exist: "+folderName), null);
									} else {
										try {
											folder.open(Folder.READ_WRITE);
											Message[] messages=folder.getMessages();
											for(int f=0;f<messages.length;f++) {
												Message message=messages[f];
												if(!message.isSet(Flags.Flag.DELETED)) {
													// The list of emails notified of ticket changes
													StringBuilder notifyEmails=new StringBuilder();
													List<String> notifyUsed=new SortedArrayList<String>();

													// Get the from addresses
													Address[] fromAddresses=message.getFrom();
													List<String> froms=new ArrayList<String>(fromAddresses==null ? 0 : fromAddresses.length);
													if(fromAddresses!=null) {
														for(int d=0;d<fromAddresses.length;d++) {
															Address addy=fromAddresses[d];
															String S;
															if(addy instanceof InternetAddress) S=((InternetAddress)addy).getAddress().toLowerCase();
															else S=addy.toString().toLowerCase();
															froms.add(S);
															if(!notifyUsed.contains(S)) {
																if(notifyEmails.length()>0) notifyEmails.append('\n');
																notifyEmails.append(S);
																notifyUsed.add(S);
															}
														}
													}
													// Get the to addresses
													Address[] toAddresses=message.getAllRecipients();
													List<String> tos=new ArrayList<String>(toAddresses==null ? 0 : toAddresses.length);
													if(toAddresses!=null) {
														for(int d=0;d<toAddresses.length;d++) {
															Address addy=toAddresses[d];
															String S;
															if(addy instanceof InternetAddress) S=((InternetAddress)addy).getAddress().toLowerCase();
															else S=addy.toString().toLowerCase();
															// Skip if in the ignore list
															boolean ignored=false;
															for(int e=0;e<ignore_recipients.size();e++) {
																String ignoredAddy=ignore_recipients.get(e);
																if(ignoredAddy.equalsIgnoreCase(S)) {
																	ignored=true;
																	break;
																}
															}
															if(!ignored) {
																tos.add(S);
																if(!notifyUsed.contains(S)) {
																	if(notifyEmails.length()>0) notifyEmails.append('\n');
																	notifyEmails.append(S);
																	notifyUsed.add(S);
																}
															}
														}
													}

													// Try to guess the business ownership, but never guess for a cancelled business
													String account=AccountHandler.getAccountFromEmailAddresses(conn, froms);

													String emailBody=getMessageBody(message);

													// Add ticket
													addTicket(
														conn,
														invalidateList,
														account,
														null,
														TicketType.NONE,
														emailBody,
														-1,
														Priority.NORMAL,
														Priority.NORMAL,
														null,
														assign_to==null || assign_to.length()==0 ? null : assign_to,
														notifyEmails.toString(),
														""
													);

													// Commit individual ticket
													conn.commit();
												}
											}
											// Archive all messages
											if (archiveFolderName!=null && (archiveFolderName=archiveFolderName.trim()).length()>0) {
												Folder archiveFolder=store.getFolder(archiveFolderName);
												try {
													if (!archiveFolder.exists()) {
														UserHost.reportWarning(new IOException("Folder does not exist: "+archiveFolderName), null);
													} else {
														folder.copyMessages(messages, archiveFolder);
													}
												} finally {
													if (archiveFolder.isOpen()) archiveFolder.close(true);
												}
											}
											// Delete all messages
											folder.setFlags(messages, new Flags(Flags.Flag.DELETED), true);
											folder.expunge();
										} finally {
											if(folder.isOpen()) folder.close(true);
										}
									}
								} finally {
									if(store.isConnected()) store.close();
								}
							}
							conn.commit();
							MasterServer.invalidateTables(conn, invalidateList, null);
						}
					} finally {
						timer.stop();
					}
				}
			} catch(ThreadDeath TD) {
				throw TD;
			} catch(Throwable T) {
				logger.log(Level.SEVERE, null, T);
			}
		}
	}
*/
	// private static final String[] MATCH_HEADERS=new String[]{"Subject", "Date", "From", "Cc"};

	/**
	 * Gets the String form of the message body.
	 */
	/*
	public String getMessageBody(Message message) throws IOException, MessagingException {
		try {
			StringBuilder SB=new StringBuilder();
			Enumeration headers=message.getMatchingHeaders(MATCH_HEADERS);
			while(headers.hasMoreElements()) {
				Header h=(Header)headers.nextElement();
				String name=h.getName();
				String val=h.getValue();
				if (val!=null && (val=val.trim()).length()>0)
					SB.append(name).append(": ").append(val).append('\n');
			}
			if(SB.length()>0) SB.append('\n');
			getMessageBody0(message.getContent(), SB);
			return SB.toString();
		} catch(UnsupportedEncodingException err) {
			logger.log(Level.WARNING, null, err);
			return message.getContent().toString();
		}
	}

	private void getMessageBody0(Object content, StringBuilder SB) throws IOException, MessagingException {
		if(content instanceof MimeMultipart) {
			MimeMultipart mpart = (MimeMultipart)content;
			int partCount = mpart.getCount();
			for (int i = 0; i<partCount; i++) {
				getMessageBody0(mpart.getBodyPart(i), SB);
			}
		} else if(content instanceof String) {
			if(SB.length()>0) SB.append(PART_SEPARATOR);
			SB.append(Strings.wordWrap((String)content, 100));
		} else if(content instanceof Part) {
			Part part=(Part)content;
			if(part.isMimeType("multipart/*")) {
				getMessageBody0(part.getContent(), SB);
			} else {
				if(SB.length()>0) SB.append(PART_SEPARATOR);
				SB.append(Strings.wordWrap(part.getContent().toString(), 100));
			}
		} else {
			if(SB.length()>0) SB.append(PART_SEPARATOR);
			SB.append(Strings.wordWrap(content.toString(), 100));
		}
	}*/
	// </editor-fold>
}
