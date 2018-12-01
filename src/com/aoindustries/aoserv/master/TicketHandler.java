/*
 * Copyright 2001-2013, 2015, 2017, 2018 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.master.Permission;
import com.aoindustries.aoserv.client.master.User;
import com.aoindustries.aoserv.client.schema.Table;
import com.aoindustries.aoserv.client.ticket.ActionType;
import com.aoindustries.aoserv.client.ticket.Status;
import com.aoindustries.aoserv.client.ticket.TicketType;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.aoserv.client.validator.UserId;
import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.DatabaseAccess;
import com.aoindustries.dbc.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;
import java.util.logging.Level;

/**
 * The <code>TicketHandler</code> handles all the accesses to the ticket tables.
 *
 * @author  AO Industries, Inc.
 */
final public class TicketHandler /*implements Runnable*/ {

    private TicketHandler() {
    }

    // <editor-fold desc="Security">
    /**
     * To be able to access a ticket action, must both have access to its 
     * @param conn
     * @param source
     * @param action
     * @return
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public static boolean canAccessTicketAction(DatabaseConnection conn, RequestSource source, int action) throws IOException, SQLException {
        User mu = MasterServer.getUser(conn, source.getUsername());
        if(mu!=null) {
            if(MasterServer.getUserHosts(conn, source.getUsername()).length==0) return true; // Master users
            else return false; // daemons
        } else {
            AccountingCode accounting = getBusinessForAction(conn, action);
            if(isTicketAdmin(conn, source)) {
                // Must have access to either the accounting or reseller
                if(accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting)) return true;
                AccountingCode reseller = getResellerForAction(conn, action);
                return BusinessHandler.canAccessBusiness(conn, source, reseller);
            } else {
                // Must have access to the business and may not be an admin-only action
                if(accounting==null || !BusinessHandler.canAccessBusiness(conn, source, accounting)) return false;
                return !getVisibleAdminOnlyForAction(conn, action);
            }
        }
    }

    public static void checkAccessTicketAction(DatabaseConnection conn, RequestSource source, String verb, int action) throws IOException, SQLException {
        if(!canAccessTicketAction(conn, source, action)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access ticket_action: verb='"
                +verb
                +", action="
                +action
            ;
            throw new SQLException(message);
        }
    }

    public static boolean canAccessTicket(DatabaseConnection conn, RequestSource source, int ticketId) throws IOException, SQLException {
        User mu = MasterServer.getUser(conn, source.getUsername());
        if(mu!=null) {
            if(MasterServer.getUserHosts(conn, source.getUsername()).length==0) {
                // Master users
                return true;
            } else {
                // daemons

                // Can only access their own logs ticket.Ticket
                AccountingCode baAccounting = UsernameHandler.getBusinessForUsername(conn, source.getUsername());
                String status = getStatusForTicket(conn, ticketId);
                return
                    baAccounting.equals(getBrandForTicket(conn, ticketId))
                    && baAccounting.equals(getBusinessForTicket(conn, ticketId))
                    && (
                        Status.OPEN.equals(status)
                        || Status.HOLD.equals(status)
                        || Status.BOUNCED.equals(status)
                    )
                    && TicketType.LOGS.equals(getTicketTypeForTicket(conn, ticketId))
                ;
            }
        } else {
            AccountingCode accounting = getBusinessForTicket(conn, ticketId);
            if(isTicketAdmin(conn, source)) {
                // Must have access to either the accounting or reseller
                if(accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting)) return true;
                AccountingCode reseller = getResellerForTicket(conn, ticketId);
                return BusinessHandler.canAccessBusiness(conn, source, reseller);
            } else {
                // Must have access to the business
                return accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting);
            }
        }
    }

    public static void checkAccessTicket(DatabaseConnection conn, RequestSource source, String action, int ticket) throws IOException, SQLException {
        if(!canAccessTicket(conn, source, ticket)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
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
        Random random = MasterServer.getRandom();
        for(int range=1000000; range<1000000000; range *= 10) {
            for(int attempt=0; attempt<1000; attempt++) {
                int id = random.nextInt(range);
                if(conn.executeBooleanQuery("select (select id from ticket.\"Ticket\" where id=?) is null", id)) return id;
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
        AccountingCode brand,
        AccountingCode accounting,
        String language,
        int category,
        String type,
        String fromAddress,
        String summary,
        String details,
        String clientPriority,
        String contactEmails,
        String contactPhoneNumbers
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "addTicket", Permission.Name.add_ticket);
        boolean isAdmin=isTicketAdmin(conn, source);
        if(accounting==null) {
            // if(!isAdmin) throw new SQLException("Only ticket administrators may create a ticket without a business.");
        } else {
            BusinessHandler.checkAccessBusiness(conn, source, "addTicket", accounting);
            if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add Ticket, Account disabled: "+accounting);
        }
        AccountingCode reseller = ResellerHandler.getResellerForBusinessAutoEscalate(
            conn,
            accounting==null ? UsernameHandler.getBusinessForUsername(conn, source.getUsername()) : accounting
        );
        return addTicket(
            conn,
            invalidateList,
            brand,
            reseller,
            accounting,
            language,
            source.getUsername(),
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
        AccountingCode brand,
        AccountingCode reseller,
        AccountingCode accounting,
        String language,
        UserId createdBy,
        int category,
        String type,
        String fromAddress,
        String summary,
        String details,
        String rawEmail,
        String clientPriority,
        String adminPriority,
        String status,
        long statusTimeout,
        String contactEmails,
        String contactPhoneNumbers,
        String internalNotes
    ) throws IOException, SQLException {
        int id = generateTicketId(conn);

        conn.executeUpdate(
            "insert into ticket.\"Ticket\" values(?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,?,?,?,?,?)",
            id,
            brand,
            reseller,
            accounting,
            language,
            createdBy,
            category==-1 ? DatabaseAccess.Null.INTEGER : category,
            type,
            fromAddress,
            summary,
            details,
            rawEmail,
            clientPriority,
            adminPriority,
            status,
            statusTimeout==-1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(statusTimeout),
            contactEmails,
            contactPhoneNumbers,
            internalNotes
        );

        /*
        if(
            clientPriority.equals(Priority.URGENT)
            && !accounting.equals(BusinessHandler.getRootBusiness()))
        {
            String message =
                "Account:   "+accounting+"\n"+
                "Username:   "+username+"\n"+
                "Type:       "+type+"\n"+
                "Deadline:   "+(deadline==-1?"":SQLUtility.getDate(deadline))+"\n"+
                "Technology: "+(technology==null?"":technology)+"\n"+
                "\n\n"+
                details+"\n\n"+
                MasterConfiguration.getTicketURL()+id
            ;
            sendEmail("support@aoindustries.com", "URGENT ticket notification", message, "ID: "+id+" "+details);
        }*/

        // Notify all clients of the updates
        if(accounting!=null) invalidateList.addTable(conn, Table.TableID.TICKETS, accounting, InvalidateList.allServers, false);
        invalidateList.addTable(conn, Table.TableID.TICKETS, brand, InvalidateList.allServers, false);
        invalidateList.addTable(conn, Table.TableID.TICKETS, reseller, InvalidateList.allServers, false);
        //invalidateList.addTable(conn, Table.TableID.ACTIONS, accounting, null);
        return id;
    }
    // </editor-fold>
    // <editor-fold desc="Delayed Data Access">
    public static String getTicketDetails(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketDetails", ticket);
        return conn.executeStringQuery("select details from ticket.\"Ticket\" where id=?", ticket);
    }

    public static String getTicketRawEmail(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketRawEmail", ticket);
        return conn.executeStringQuery("select raw_email from ticket.\"Ticket\" where id=?", ticket);
    }

    public static String getTicketInternalNotes(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketInternalNotes", ticket);
        if(isTicketAdmin(conn, source)) return conn.executeStringQuery("select internal_notes from ticket.\"Ticket\" where id=?", ticket);
        else return "";
    }

    public static String getTicketActionOldValue(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionOldValue", ticket);
        return conn.executeStringQuery("select old_value from ticket.\"Action\" where id=?", ticket);
    }

    public static String getTicketActionNewValue(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionNewValue", ticket);
        return conn.executeStringQuery("select new_value from ticket.\"Action\" where id=?", ticket);
    }

    public static String getTicketActionDetails(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionDetails", ticket);
        return conn.executeStringQuery("select details from ticket.\"Action\" where id=?", ticket);
    }

    public static String getTicketActionRawEmail(
        DatabaseConnection conn,
        RequestSource source,
        int ticket
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionRawEmail", ticket);
        return conn.executeStringQuery("select raw_email from ticket.\"Action\" where id=?", ticket);
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

        conn.executeUpdate("update ticket.\"Ticket\" set status=? where id=?", Status.BOUNCED, ticket);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            ActionType.BOUNCED,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
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
        String oldValue=conn.executeStringQuery("select admin_priority from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set admin_priority=? where id=?", priority, ticket);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
            ticket,
            username,
            ActionType.ADMIN_PRIORITY_CHANGE,
            oldValue,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
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
        String oldValue=conn.executeStringQuery("select assigned_to from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set assigned_to=? where id=?", assignedTo, ticket);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
            ticket,
            username,
            ActionType.ASSIGNED,
            oldValue,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );
    }
*/
    public static boolean setTicketBusiness(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticket,
        AccountingCode oldAccounting,
        AccountingCode newAccounting
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketBusiness", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketBusiness", ticket);

        if(newAccounting!=null) BusinessHandler.checkAccessBusiness(conn, source, "setTicketBusiness", newAccounting);

        int updateCount;
        if(oldAccounting==null) updateCount = conn.executeUpdate("update ticket.\"Ticket\" set accounting=? where id=? and accounting is null", newAccounting, ticket);
        else updateCount = conn.executeUpdate("update ticket.\"Ticket\" set accounting=? where id=? and accounting=?", newAccounting, ticket, oldAccounting);

        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket.\"Action\"(ticket, administrator, action_type, old_accounting, new_accounting) values(?,?,?,?,?)",
                ticket,
                source.getUsername(),
                ActionType.SET_BUSINESS,
                oldAccounting,
                newAccounting
            );

            // Notify all clients of the update
            // By oldAccounting
            if(oldAccounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    oldAccounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKET_ACTIONS,
                    oldAccounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By newAccounting
            if(newAccounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    newAccounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKET_ACTIONS,
                    newAccounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            AccountingCode brand = conn.executeObjectQuery(
                ObjectFactories.accountingCodeFactory,
                "select brand from ticket.\"Ticket\" where id=?",
                ticket
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            AccountingCode reseller = conn.executeObjectQuery(
                ObjectFactories.accountingCodeFactory,
                "select reseller from ticket.\"Ticket\" where id=?",
                ticket
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                reseller,
                InvalidateList.allServers,
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
        BusinessHandler.checkPermission(conn, source, "setTicketType", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketType", ticket);

        int updateCount = conn.executeUpdate(
            "update ticket.\"Ticket\" set ticket_type=? where id=? and ticket_type=?",
            newType,
            ticket,
            oldType
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket.\"Action\"(ticket, administrator, action_type, old_type, new_type) values(?,?,?,?,?)",
                ticket,
                source.getUsername(),
                ActionType.SET_TYPE,
                oldType,
                newType
            );

            // Notify all clients of the update
            // By accounting
            AccountingCode accounting = getBusinessForTicket(conn, ticket);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            AccountingCode brand = getBrandForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            AccountingCode reseller = getResellerForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                reseller,
                InvalidateList.allServers,
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
        BusinessHandler.checkPermission(conn, source, "setTicketStatus", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketStatus", ticket);

        int updateCount = conn.executeUpdate(
            "update ticket.\"Ticket\" set status=?, status_timeout=? where id=? and status=?",
            newStatus,
            statusTimeout==-1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(statusTimeout),
            ticket,
            oldStatus
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket.\"Action\"(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
                ticket,
                source.getUsername(),
                ActionType.SET_STATUS,
                oldStatus,
                newStatus
            );

            // Notify all clients of the update
            // By accounting
            AccountingCode accounting = getBusinessForTicket(conn, ticket);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            AccountingCode brand = getBrandForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            AccountingCode reseller = getResellerForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                reseller,
                InvalidateList.allServers,
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
        BusinessHandler.checkPermission(conn, source, "setTicketInternalNotes", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketInternalNotes", ticket);

        int updateCount = conn.executeUpdate(
            "update ticket.\"Ticket\" set internal_notes=? where id=? and internal_notes=?",
            newInternalNotes,
            ticket,
            oldInternalNotes
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
                ticket,
                source.getUsername(),
                ActionType.SET_INTERNAL_NOTES,
                oldInternalNotes,
                newInternalNotes
            );

            // Notify all clients of the update
            // By accounting
            AccountingCode accounting = getBusinessForTicket(conn, ticket);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            AccountingCode brand = getBrandForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            AccountingCode reseller = getResellerForTicket(conn, ticket);
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                reseller,
                InvalidateList.allServers,
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
        String contactEmails
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketContactEmails", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketContactEmails", ticket);

        String oldValue = conn.executeStringQuery("select contact_emails from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set contact_emails=? where id=?", contactEmails, ticket);

        conn.executeUpdate(
            "insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticket,
            source.getUsername(),
            ActionType.SET_CONTACT_EMAILS,
            oldValue,
            contactEmails
        );

        // Notify all clients of the update
        // By accounting
        AccountingCode accounting = getBusinessForTicket(conn, ticket);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        AccountingCode brand = getBrandForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        AccountingCode reseller = getResellerForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void setTicketContactPhoneNumbers(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticket,
        String contactPhoneNumbers
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketContactPhoneNumbers", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketContactPhoneNumbers", ticket);

        String oldValue = conn.executeStringQuery("select contact_phone_numbers from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set contact_phone_numbers=? where id=?", contactPhoneNumbers, ticket);

        conn.executeUpdate(
            "insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticket,
            source.getUsername(),
            ActionType.SET_CONTACT_PHONE_NUMBERS,
            oldValue,
            contactPhoneNumbers
        );

        // Notify all clients of the update
        // By accounting
        AccountingCode accounting = getBusinessForTicket(conn, ticket);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        AccountingCode brand = getBrandForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        AccountingCode reseller = getResellerForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void changeTicketClientPriority(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticket, 
        String newClientPriority
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "changeTicketClientPriority", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "changeTicketClientPriority", ticket);

        String oldClientPriority = conn.executeStringQuery("select client_priority from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set client_priority=? where id=?", newClientPriority, ticket);

        conn.executeUpdate(
            "insert into ticket.\"Action\"(ticket, administrator, action_type, old_priority, new_priority) values(?,?,?,?,?)",
            ticket,
            source.getUsername(),
            ActionType.SET_CLIENT_PRIORITY,
            oldClientPriority,
            newClientPriority
        );

        // Notify all clients of the update
        // By accounting
        AccountingCode accounting = getBusinessForTicket(conn, ticket);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        AccountingCode brand = getBrandForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        AccountingCode reseller = getResellerForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void setTicketSummary(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticket,
        String summary
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketSummary", Permission.Name.edit_ticket);
        checkAccessTicket(conn, source, "setTicketSummary", ticket);

        String oldValue = conn.executeStringQuery("select summary from ticket.\"Ticket\" where id=?", ticket);

        conn.executeUpdate("update ticket.\"Ticket\" set summary=? where id=?", summary, ticket);

        conn.executeUpdate(
            "insert into ticket.\"Action\"(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticket,
            source.getUsername(),
            ActionType.SET_SUMMARY,
            oldValue,
            summary
        );

        // Notify all clients of the update
        // By accounting
        AccountingCode accounting = getBusinessForTicket(conn, ticket);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        AccountingCode brand = getBrandForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        AccountingCode reseller = getResellerForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
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
        BusinessHandler.checkPermission(conn, source, "addTicketAnnotation", Permission.Name.add_ticket);
        checkAccessTicket(conn, source, "addTicketAnnotation", ticket);

        addTicketAnnotation(conn, invalidateList, ticket, source.getUsername(), summary, details);
    }

    /**
     * Adds an annotation <b>without</b> security checks.
     */
    public static void addTicketAnnotation(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int ticket,
        UserId administrator,
        String summary,
        String details
    ) throws IOException, SQLException {
        conn.executeUpdate(
            "insert into ticket.\"Action\"(ticket, administrator, action_type, summary, details) values(?,?,?,?,?)",
            ticket,
            administrator,
            ActionType.ADD_ANNOTATION,
            summary,
            details
        );
        // By accounting
        AccountingCode accounting = getBusinessForTicket(conn, ticket);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                Table.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        AccountingCode brand = getBrandForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        AccountingCode reseller = getResellerForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );

        // Reopen if needed
        String status = conn.executeStringQuery("select status from ticket.\"Ticket\" where id=?", ticket);
        if(
            status.equals(Status.BOUNCED)
            || status.equals(Status.CLOSED)
        ) {
            conn.executeUpdate("update ticket.\"Ticket\" set status=?, status_timeout=null where id=?", Status.OPEN, ticket);
            conn.executeUpdate(
                "insert into ticket.\"Action\"(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
                ticket,
                administrator,
                ActionType.SET_STATUS,
                status,
                Status.OPEN
            );
            // By accounting
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    Table.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            invalidateList.addTable(
                conn,
                Table.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
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

        conn.executeUpdate(
            "update ticket.\"Ticket\" set close_date=now(), closed_by=?, status=?, assigned_to=null where id=?",
            username,
            Status.COMPLETED,
            ticket
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            ActionType.COMPLETE_TICKET,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
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

        String baAccounting = UsernameHandler.getBusinessForUsername(conn, username);
        boolean isAdminChange=baAccounting.equals(BusinessHandler.getRootBusiness());

        conn.executeUpdate(
            "update ticket.\"Ticket\" set status=? where id=?",
            isAdminChange?Status.ADMIN_HOLD:Status.CLIENT_HOLD,
            ticket
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            isAdminChange?ActionType.ADMIN_HOLD:ActionType.CLIENT_HOLD,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
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

        String accounting1 = conn.executeStringQuery("select pk.accounting from account.\"Username\" un, billing.\"Package\" pk where un.username=? and un.package=pk.name", username);
        String accounting2 = conn.executeStringQuery("select accounting from ticket.\"Ticket\" where id=?", ticket);

        boolean isClientChange=accounting1.equals(accounting2);

        conn.executeUpdate(
            "update ticket.\"Ticket\" set close_date=now(), closed_by=?, status=?, assigned_to=null where id=?",
            username,
            isClientChange?Status.CLIENT_KILL:Status.ADMIN_KILL,
            ticket
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            isClientChange?ActionType.CLIENT_KILLED:ActionType.ADMIN_KILL,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );
*/
        /*
        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
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

        conn.executeUpdate("update ticket.\"Ticket\" set status=? where id=?", Status.UNDERWAY, ticket);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            ActionType.REACTIVATE_TICKET,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
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

        conn.executeUpdate("update ticket.\"Ticket\" set status=? where id=?", Status.UNDERWAY, ticket);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticket,
            username,
            ActionType.WORK_ENTRY,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticket);
        invalidateList.addTable(
            conn,
            Table.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            Table.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
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
        return conn.executeBooleanQuery("select (select accounting from reseller.\"Reseller\" where accounting=?) is not null", UsernameHandler.getBusinessForUsername(conn, source.getUsername()));
    }

    public static AccountingCode getBusinessForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select accounting from ticket.\"Ticket\" where id=?",
            ticket
        );
    }

    public static AccountingCode getBrandForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select brand from ticket.\"Ticket\" where id=?",
            ticket
        );
    }

    public static String getStatusForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
        return conn.executeStringQuery("select status from ticket.\"Ticket\" where id=?", ticket);
    }

    public static String getTicketTypeForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
        return conn.executeStringQuery("select ticket_type from ticket.\"Ticket\" where id=?", ticket);
    }

    public static AccountingCode getResellerForTicket(DatabaseConnection conn, int ticket) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select reseller from ticket.\"Ticket\" where id=?",
            ticket
        );
    }

    public static AccountingCode getBusinessForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select ti.accounting from ticket.\"Action\" ac, ticket.\"Ticket\" ti where ac.id=? and ac.ticket=ti.id",
            action
        );
    }

    public static AccountingCode getResellerForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
        return conn.executeObjectQuery(
            ObjectFactories.accountingCodeFactory,
            "select ti.reseller from ticket.\"Action\" ac, ticket.\"Ticket\" ti where ac.id=? and ac.ticket=ti.id",
            action
        );
    }

    public static boolean getVisibleAdminOnlyForAction(DatabaseConnection conn, int action) throws IOException, SQLException {
        return conn.executeBooleanQuery("select tat.visible_admin_only from ticket.\"Action\" ac inner join ticket.\"ActionType\" tat on ac.action_type=tat.type where ac.id=?", action);
    }

    /*
    public static boolean isActive(DatabaseConnection conn, int ticketId) throws IOException, SQLException {
        String status=conn.executeStringQuery("select status from ticket.\"Ticket\" where id=?", ticketId);
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
            List<String> tos = StringUtility.splitStringCommaSpace(to);
            int len = tos.size();
            for (int i = 0; i<len; i++) {
                try {
                    String[] orAddys=StringUtility.splitString(tos.get(i), '|');
                    if(orAddys.length>0) {
                        MailMessage msg=new MailMessage(MasterConfiguration.getTicketSmtpServer());
                        msg.from("support@aoindustries.com");
                        msg.to(orAddys[MasterServer.getRandom().nextInt(orAddys.length)]);
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

    public static String getContactEmails(DatabaseConnection conn, int id) throws IOException, SQLException {
        String contactEmails=conn.executeStringQuery(
            "select contact_emails from ticket.\"Ticket\" where id=?",
            id
        );
        if(contactEmails.length()==0) return BusinessHandler.getTechnicalEmail(conn, getBusinessForTicket(conn, id));
        return contactEmails;
    }

    private static Thread thread;
     */
    private static boolean cronDaemonAdded = false;

	/**
	 * Runs once every four hours
	 */
    private static final Schedule schedule = (minute, hour, dayOfMonth, month, dayOfWeek, year) -> minute==25 && (hour&3)==3;

    public static void start() {
        synchronized(System.out) {
            if(!cronDaemonAdded) {
                System.out.print("Starting " + TicketHandler.class.getSimpleName() + ": ");
                CronDaemon.addCronJob(
                    new CronJob() {
						@Override
                        public Schedule getCronJobSchedule() {
                            return schedule;
                        }

						@Override
                        public CronJobScheduleMode getCronJobScheduleMode() {
                            return CronJobScheduleMode.SKIP;
                        }

						@Override
                        public String getCronJobName() {
                            return "Clean log ticket.Ticket";
                        }

						@Override
                        public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
                            try {
                                InvalidateList invalidateList = new InvalidateList();
                                MasterDatabase database = MasterDatabase.getDatabase();
                                int updateCount = database.executeUpdate(
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
                                        database,
                                        Table.TableID.TICKET_ACTIONS,
                                        InvalidateList.allBusinesses,
                                        InvalidateList.allServers,
                                        false
                                    );
                                }
                                updateCount = database.executeUpdate(
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
                                        database,
                                        Table.TableID.TICKETS,
                                        InvalidateList.allBusinesses,
                                        InvalidateList.allServers,
                                        false
                                    );
                                }
                                MasterServer.invalidateTables(invalidateList, null);
                            } catch(ThreadDeath TD) {
                                throw TD;
                            } catch(Throwable T) {
                                LogFactory.getLogger(getClass()).log(Level.SEVERE, null, T);
                            }
                        }

						@Override
                        public int getCronJobThreadPriority() {
                            return Thread.NORM_PRIORITY-2;
                        }
                    }, LogFactory.getLogger(TicketHandler.class)
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
                        MasterServer.getRandom(),
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
                        InvalidateList invalidateList=new InvalidateList();
                        DatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
                        try {
                            boolean connRolledBack=false;
                            try {
                                for(int c=1;c<Integer.MAX_VALUE;c++) {
                                    String hostname=MasterConfiguration.getTicketSource("imap", c, "hostname");
                                    if(hostname==null) break;
                                    String username=MasterConfiguration.getTicketSource("imap", c, "username");
                                    String password=MasterConfiguration.getTicketSource("imap", c, "password");
                                    String folderName=MasterConfiguration.getTicketSource("imap", c, "folder");
                                    String archiveFolderName=MasterConfiguration.getTicketSource("imap", c, "archivefolder");
                                    List<String> ignore_recipients=StringUtility.splitStringCommaSpace(MasterConfiguration.getTicketSource("imap", c, "ignore_recipients"));
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
                                                        String accounting=BusinessHandler.getBusinessFromEmailAddresses(conn, froms);

                                                        String emailBody=getMessageBody(message);

                                                        // Add ticket
                                                        addTicket(
                                                            conn,
                                                            invalidateList,
                                                            accounting,
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
                            } catch(RuntimeException err) {
                                if(conn.rollback()) {
                                    connRolledBack=true;
                                }
                                throw err;
                            } catch(IOException err) {
                                if(conn.rollback()) {
                                    connRolledBack=true;
                                }
                                throw err;
                            } catch(SQLException err) {
                                if(conn.rollbackAndClose()) {
                                    connRolledBack=true;
                                }
                                throw err;
                            } finally {
                                if(!connRolledBack && !conn.isClosed()) conn.commit();
                            }
                        } finally {
                            conn.releaseConnection();
                        }
                        MasterServer.invalidateTables(invalidateList, null);
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
    // private static final String[] MATCH_HEADERS=new String[]{"Subject","Date","From","Cc"};
    
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
            SB.append(StringUtility.wordWrap((String)content, 100));
        } else if(content instanceof Part) {
            Part part=(Part)content;
            if(part.isMimeType("multipart/*")) {
                getMessageBody0(part.getContent(), SB);
            } else {
                if(SB.length()>0) SB.append(PART_SEPARATOR);
                SB.append(StringUtility.wordWrap(part.getContent().toString(), 100));
            }
        } else {
            if(SB.length()>0) SB.append(PART_SEPARATOR);
            SB.append(StringUtility.wordWrap(content.toString(), 100));
        }
    }*/
    // </editor-fold>
}
