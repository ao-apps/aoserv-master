package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.AOServPermission;
import com.aoindustries.aoserv.client.MasterUser;
import com.aoindustries.aoserv.client.SchemaTable;
import com.aoindustries.aoserv.client.TicketActionType;
import com.aoindustries.aoserv.client.TicketStatus;
import com.aoindustries.aoserv.client.TicketType;
import com.aoindustries.sql.DatabaseAccess;
import com.aoindustries.sql.DatabaseConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

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
     * @param actionId
     * @return
     * @throws java.io.IOException
     * @throws java.sql.SQLException
     */
    public static boolean canAccessTicketAction(DatabaseConnection conn, RequestSource source, int actionId) throws IOException, SQLException {
        MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
        if(mu!=null) {
            if(MasterServer.getMasterServers(conn, source.getUsername()).length==0) return true; // Master users
            else return false; // daemons
        } else {
            String accounting = getBusinessForAction(conn, actionId);
            if(isTicketAdmin(conn, source)) {
                // Must have access to either the accounting or reseller
                if(accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting)) return true;
                String reseller = getResellerForAction(conn, actionId);
                return BusinessHandler.canAccessBusiness(conn, source, reseller);
            } else {
                // Must have access to the business and may not be an admin-only action
                if(accounting==null || !BusinessHandler.canAccessBusiness(conn, source, accounting)) return false;
                return !getVisibleAdminOnlyForAction(conn, actionId);
            }
        }
    }

    public static void checkAccessTicketAction(DatabaseConnection conn, RequestSource source, String action, int actionId) throws IOException, SQLException {
        if(!canAccessTicketAction(conn, source, actionId)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access ticket_action: action='"
                +action
                +", pkey="
                +actionId
            ;
            throw new SQLException(message);
        }
    }

    public static boolean canAccessTicket(DatabaseConnection conn, RequestSource source, int ticketId) throws IOException, SQLException {
        MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
        if(mu!=null) {
            if(MasterServer.getMasterServers(conn, source.getUsername()).length==0) {
                // Master users
                return true;
            } else {
                // daemons

                // Can only access their own logs tickets
                String baAccounting = UsernameHandler.getBusinessForUsername(conn, source.getUsername());
                String status = getStatusForTicket(conn, ticketId);
                return
                    baAccounting.equals(getBrandForTicket(conn, ticketId))
                    && baAccounting.equals(getBusinessForTicket(conn, ticketId))
                    && (
                        TicketStatus.OPEN.equals(status)
                        || TicketStatus.HOLD.equals(status)
                        || TicketStatus.BOUNCED.equals(status)
                    )
                    && TicketType.LOGS.equals(getTicketTypeForTicket(conn, ticketId))
                ;
            }
        } else {
            String accounting = getBusinessForTicket(conn, ticketId);
            if(isTicketAdmin(conn, source)) {
                // Must have access to either the accounting or reseller
                if(accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting)) return true;
                String reseller = getResellerForTicket(conn, ticketId);
                return BusinessHandler.canAccessBusiness(conn, source, reseller);
            } else {
                // Must have access to the business
                return accounting!=null && BusinessHandler.canAccessBusiness(conn, source, accounting);
            }
        }
    }

    public static void checkAccessTicket(DatabaseConnection conn, RequestSource source, String action, int ticketID) throws IOException, SQLException {
        if(!canAccessTicket(conn, source, ticketID)) {
            String message=
                "business_administrator.username="
                +source.getUsername()
                +" is not allowed to access ticket: action='"
                +action
                +", pkey="
                +ticketID
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
                if(conn.executeBooleanQuery("select (select pkey from tickets where pkey=?) is null", id)) return id;
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
        String brand,
        String accounting,
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
        BusinessHandler.checkPermission(conn, source, "addTicket", AOServPermission.Permission.add_ticket);
        boolean isAdmin=isTicketAdmin(conn, source);
        if(accounting==null) {
            // if(!isAdmin) throw new SQLException("Only ticket administrators may create a ticket without a business.");
        } else {
            BusinessHandler.checkAccessBusiness(conn, source, "addTicket", accounting);
            if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add Ticket, Business disabled: "+accounting);
        }
        String reseller = ResellerHandler.getResellerForBusinessAutoEscalate(
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
            TicketStatus.OPEN,
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
        String brand,
        String reseller,
        String accounting,
        String language,
        String createdBy,
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
        int pkey = generateTicketId(conn);

        conn.executeUpdate(
            "insert into tickets values(?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,?,?,?,?,?)",
            pkey,
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
            clientPriority.equals(TicketPriority.URGENT)
            && !accounting.equals(BusinessHandler.getRootBusiness()))
        {
            String message =
                "Business:   "+accounting+"\n"+
                "Username:   "+username+"\n"+
                "Type:       "+type+"\n"+
                "Deadline:   "+(deadline==-1?"":SQLUtility.getDate(deadline))+"\n"+
                "Technology: "+(technology==null?"":technology)+"\n"+
                "\n\n"+
                details+"\n\n"+
                MasterConfiguration.getTicketURL()+pkey
            ;
            sendEmail("support@aoindustries.com", "URGENT ticket notification", message, "ID: "+pkey+" "+details);
        }*/

        // Notify all clients of the updates
        if(accounting!=null) invalidateList.addTable(conn, SchemaTable.TableID.TICKETS, accounting, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.TableID.TICKETS, brand, InvalidateList.allServers, false);
        invalidateList.addTable(conn, SchemaTable.TableID.TICKETS, reseller, InvalidateList.allServers, false);
        //invalidateList.addTable(conn, SchemaTable.TableID.ACTIONS, accounting, null);
        return pkey;
    }
    // </editor-fold>
    // <editor-fold desc="Delayed Data Access">
    public static String getTicketDetails(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketDetails", pkey);
        return conn.executeStringQuery("select details from tickets where pkey=?", pkey);
    }

    public static String getTicketRawEmail(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketRawEmail", pkey);
        return conn.executeStringQuery("select raw_email from tickets where pkey=?", pkey);
    }

    public static String getTicketInternalNotes(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "getTicketInternalNotes", pkey);
        if(isTicketAdmin(conn, source)) return conn.executeStringQuery("select internal_notes from tickets where pkey=?", pkey);
        else return "";
    }

    public static String getTicketActionOldValue(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionOldValue", pkey);
        return conn.executeStringQuery("select old_value from ticket_actions where pkey=?", pkey);
    }

    public static String getTicketActionNewValue(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionNewValue", pkey);
        return conn.executeStringQuery("select new_value from ticket_actions where pkey=?", pkey);
    }

    public static String getTicketActionDetails(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionDetails", pkey);
        return conn.executeStringQuery("select details from ticket_actions where pkey=?", pkey);
    }

    public static String getTicketActionRawEmail(
        DatabaseConnection conn,
        RequestSource source,
        int pkey
    ) throws IOException, SQLException {
        checkAccessTicketAction(conn, source, "getTicketActionRawEmail", pkey);
        return conn.executeStringQuery("select raw_email from ticket_actions where pkey=?", pkey);
    }
    // </editor-fold>
    // <editor-fold desc="Ticket Actions">
    /*

    public static void bounceTicket(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "bounceTicket", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "bounceTicket", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to bounce a Ticket as user '"+LinuxAccount.MAIL+'\'');

        conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.BOUNCED, ticketID);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            TicketActionType.BOUNCED,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
            String to = getContactEmails(conn, ticketID);
            String message =
                "AO Industries support has returned Ticket "+ticketID+" to you.\n"+
                "More information is required to complete this ticket.  Please\n"+
                "browse to \n\n"+
                MasterConfiguration.getTicketURL()+ticketID+"\n\n"+
                "for more details.\n\n"+
                "--------------------------------------------------------------\n"+
                "This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
            ;
            sendEmail(to, "Ticket "+ticketID+" was bounced.", message, null);
        }
    }

    public static void changeTicketAdminPriority(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String priority, 
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "changeTicketAdminPriority", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "changeTicketAdminPriority", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change a Ticket's admin priority as user '"+LinuxAccount.MAIL+'\'');

        if(!isTicketAdmin(conn, source)) throw new SQLException("Only ticket administrators may change the administrative priority.");
        String oldValue=conn.executeStringQuery("select admin_priority from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set admin_priority=? where pkey=?", priority, ticketID);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
            ticketID,
            username,
            TicketActionType.ADMIN_PRIORITY_CHANGE,
            oldValue,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );
    }

    public static void setTicketAssignedTo(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String assignedTo, 
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "setTicketAssignedTo", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "setTicketAssignedTo", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to assign a Ticket as user '"+LinuxAccount.MAIL+'\'');

        if(!isTicketAdmin(conn, source)) throw new SQLException("Only ticket administrators may assign tickets.");
        String oldValue=conn.executeStringQuery("select assigned_to from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set assigned_to=? where pkey=?", assignedTo, ticketID);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
            ticketID,
            username,
            TicketActionType.ASSIGNED,
            oldValue,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
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
        int ticketID,
        String oldAccounting,
        String newAccounting
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketBusiness", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketBusiness", ticketID);

        if(newAccounting!=null) BusinessHandler.checkAccessBusiness(conn, source, "setTicketBusiness", newAccounting);

        int updateCount;
        if(oldAccounting==null) updateCount = conn.executeUpdate("update tickets set accounting=? where pkey=? and accounting is null", newAccounting, ticketID);
        else updateCount = conn.executeUpdate("update tickets set accounting=? where pkey=? and accounting=?", newAccounting, ticketID, oldAccounting);

        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket_actions(ticket, administrator, action_type, old_accounting, new_accounting) values(?,?,?,?,?)",
                ticketID,
                source.getUsername(),
                TicketActionType.SET_BUSINESS,
                oldAccounting,
                newAccounting
            );

            // Notify all clients of the update
            // By oldAccounting
            if(oldAccounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    oldAccounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKET_ACTIONS,
                    oldAccounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By newAccounting
            if(newAccounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    newAccounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKET_ACTIONS,
                    newAccounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
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
        int ticketID,
        String oldType,
        String newType
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketType", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketType", ticketID);

        int updateCount = conn.executeUpdate(
            "update tickets set ticket_type=? where pkey=? and ticket_type=?",
            newType,
            ticketID,
            oldType
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket_actions(ticket, administrator, action_type, old_type, new_type) values(?,?,?,?,?)",
                ticketID,
                source.getUsername(),
                TicketActionType.SET_TYPE,
                oldType,
                newType
            );

            // Notify all clients of the update
            // By accounting
            String accounting = getBusinessForTicket(conn, ticketID);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
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
        int ticketID,
        String oldStatus,
        String newStatus,
        long statusTimeout
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketStatus", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketStatus", ticketID);

        int updateCount = conn.executeUpdate(
            "update tickets set status=?, status_timeout=? where pkey=? and status=?",
            newStatus,
            statusTimeout==-1 ? DatabaseAccess.Null.TIMESTAMP : new Timestamp(statusTimeout),
            ticketID,
            oldStatus
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket_actions(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
                ticketID,
                source.getUsername(),
                TicketActionType.SET_STATUS,
                oldStatus,
                newStatus
            );

            // Notify all clients of the update
            // By accounting
            String accounting = getBusinessForTicket(conn, ticketID);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
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
        int ticketID,
        String oldInternalNotes,
        String newInternalNotes
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketInternalNotes", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketInternalNotes", ticketID);

        int updateCount = conn.executeUpdate(
            "update tickets set internal_notes=? where pkey=? and internal_notes=?",
            newInternalNotes,
            ticketID,
            oldInternalNotes
        );
        if(updateCount==1) {
            conn.executeUpdate(
                "insert into ticket_actions(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
                ticketID,
                source.getUsername(),
                TicketActionType.SET_INTERNAL_NOTES,
                oldInternalNotes,
                newInternalNotes
            );

            // Notify all clients of the update
            // By accounting
            String accounting = getBusinessForTicket(conn, ticketID);
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKET_ACTIONS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                reseller,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
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
        int ticketID,
        String contactEmails
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketContactEmails", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketContactEmails", ticketID);

        String oldValue = conn.executeStringQuery("select contact_emails from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set contact_emails=? where pkey=?", contactEmails, ticketID);

        conn.executeUpdate(
            "insert into ticket_actions(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticketID,
            source.getUsername(),
            TicketActionType.SET_CONTACT_EMAILS,
            oldValue,
            contactEmails
        );

        // Notify all clients of the update
        // By accounting
        String accounting=getBusinessForTicket(conn, ticketID);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void setTicketContactPhoneNumbers(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String contactPhoneNumbers
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketContactPhoneNumbers", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketContactPhoneNumbers", ticketID);

        String oldValue = conn.executeStringQuery("select contact_phone_numbers from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set contact_phone_numbers=? where pkey=?", contactPhoneNumbers, ticketID);

        conn.executeUpdate(
            "insert into ticket_actions(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticketID,
            source.getUsername(),
            TicketActionType.SET_CONTACT_PHONE_NUMBERS,
            oldValue,
            contactPhoneNumbers
        );

        // Notify all clients of the update
        // By accounting
        String accounting=getBusinessForTicket(conn, ticketID);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void changeTicketClientPriority(
        DatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID, 
        String newClientPriority
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "changeTicketClientPriority", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "changeTicketClientPriority", ticketID);

        String oldClientPriority = conn.executeStringQuery("select client_priority from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set client_priority=? where pkey=?", newClientPriority, ticketID);

        conn.executeUpdate(
            "insert into ticket_actions(ticket, administrator, action_type, old_priority, new_priority) values(?,?,?,?,?)",
            ticketID,
            source.getUsername(),
            TicketActionType.SET_CLIENT_PRIORITY,
            oldClientPriority,
            newClientPriority
        );

        // Notify all clients of the update
        // By accounting
        String accounting=getBusinessForTicket(conn, ticketID);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );
    }

    public static void setTicketSummary(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String summary
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "setTicketSummary", AOServPermission.Permission.edit_ticket);
        checkAccessTicket(conn, source, "setTicketSummary", ticketID);

        String oldValue = conn.executeStringQuery("select summary from tickets where pkey=?", ticketID);

        conn.executeUpdate("update tickets set summary=? where pkey=?", summary, ticketID);

        conn.executeUpdate(
            "insert into ticket_actions(ticket, administrator, action_type, old_value, new_value) values(?,?,?,?,?)",
            ticketID,
            source.getUsername(),
            TicketActionType.SET_SUMMARY,
            oldValue,
            summary
        );

        // Notify all clients of the update
        // By accounting
        String accounting=getBusinessForTicket(conn, ticketID);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                accounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            brand,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            reseller,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
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
        int ticketID,
        String summary,
        String details
    ) throws IOException, SQLException {
        BusinessHandler.checkPermission(conn, source, "addTicketAnnotation", AOServPermission.Permission.add_ticket);
        checkAccessTicket(conn, source, "addTicketAnnotation", ticketID);

        addTicketAnnotation(conn, invalidateList, ticketID, source.getUsername(), summary, details);
    }

    /**
     * Adds an annotation <b>without</b> security checks.
     */
    public static void addTicketAnnotation(
        DatabaseConnection conn,
        InvalidateList invalidateList,
        int ticketID,
        String administrator,
        String summary,
        String details
    ) throws IOException, SQLException {
        conn.executeUpdate(
            "insert into ticket_actions(ticket, administrator, action_type, summary, details) values(?,?,?,?,?)",
            ticketID,
            administrator,
            TicketActionType.ADD_ANNOTATION,
            summary,
            details
        );
        // By accounting
        String accounting=getBusinessForTicket(conn, ticketID);
        if(accounting!=null) {
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKET_ACTIONS,
                accounting,
                InvalidateList.allServers,
                false
            );
        }
        // By brand
        String brand = conn.executeStringQuery("select brand from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            brand,
            InvalidateList.allServers,
            false
        );
        // By reseller
        String reseller = conn.executeStringQuery("select reseller from tickets where pkey=?", ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKET_ACTIONS,
            reseller,
            InvalidateList.allServers,
            false
        );

        // Reopen if needed
        String status = conn.executeStringQuery("select status from tickets where pkey=?", ticketID);
        if(
            status.equals(TicketStatus.BOUNCED)
            || status.equals(TicketStatus.CLOSED)
        ) {
            conn.executeUpdate("update tickets set status=?, status_timeout=null where pkey=?", TicketStatus.OPEN, ticketID);
            conn.executeUpdate(
                "insert into ticket_actions(ticket, administrator, action_type, old_status, new_status) values(?,?,?,?,?)",
                ticketID,
                administrator,
                TicketActionType.SET_STATUS,
                status,
                TicketStatus.OPEN
            );
            // By accounting
            if(accounting!=null) {
                invalidateList.addTable(
                    conn,
                    SchemaTable.TableID.TICKETS,
                    accounting,
                    InvalidateList.allServers,
                    false
                );
            }
            // By brand
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                brand,
                InvalidateList.allServers,
                false
            );
            // By reseller
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
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
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "completeTicket", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "completeTicket", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to complete Ticket as user '"+LinuxAccount.MAIL+'\'');

        conn.executeUpdate(
            "update tickets set close_date=now(), closed_by=?, status=?, assigned_to=null where pkey=?",
            username,
            TicketStatus.COMPLETED,
            ticketID
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            TicketActionType.COMPLETE_TICKET,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
            String to = getContactEmails(conn, ticketID);
            String message =
                "AO Industries support has completed Ticket "+ticketID+".\n"+
                "Please browse to \n\n"+
                MasterConfiguration.getTicketURL()+ticketID+"\n\n"+
                "for more details.\n\n"+
                "--------------------------------------------------------------\n"+
                "This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
            ;
            sendEmail(to, "Ticket "+ticketID+" was completed.", message, null);
        }
    }

    public static void holdTicket(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "holdTicket", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        String username=source.getUsername();
        UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);

        String baAccounting = UsernameHandler.getBusinessForUsername(conn, username);
        boolean isAdminChange=baAccounting.equals(BusinessHandler.getRootBusiness());

        conn.executeUpdate(
            "update tickets set status=? where pkey=?",
            isAdminChange?TicketStatus.ADMIN_HOLD:TicketStatus.CLIENT_HOLD,
            ticketID
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            isAdminChange?TicketActionType.ADMIN_HOLD:TicketActionType.CLIENT_HOLD,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );
    }

    public static void killTicket(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "holdTicket", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to kill Ticket as user '"+LinuxAccount.MAIL+'\'');

        String accounting1 = conn.executeStringQuery("select pk.accounting from usernames un, packages pk where un.username=? and un.package=pk.name", username);
        String accounting2 = conn.executeStringQuery("select accounting from tickets where pkey=?", ticketID);

        boolean isClientChange=accounting1.equals(accounting2);

        conn.executeUpdate(
            "update tickets set close_date=now(), closed_by=?, status=?, assigned_to=null where pkey=?",
            username,
            isClientChange?TicketStatus.CLIENT_KILL:TicketStatus.ADMIN_KILL,
            ticketID
        );

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            isClientChange?TicketActionType.CLIENT_KILLED:TicketActionType.ADMIN_KILL,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );
*/
        /*
        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
            String to = getContactEmails(conn, ticketID);
            String message =
                "AO Industries support has killed Ticket "+ticketID+".\n"+
                "Please browse to \n\n"+
                MasterConfiguration.getTicketURL()+ticketID+"\n\n"+
                "for more details.\n\n"+
                "--------------------------------------------------------------\n"+
                "This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
            ;
            sendEmail(to, "Ticket "+ticketID+" was killed.", message, null);
        }*/
    //}

    /*
    public static void reactivateTicket(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "holdTicket", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to reactivate Ticket as user '"+LinuxAccount.MAIL+'\'');

        conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.UNDERWAY, ticketID);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            TicketActionType.REACTIVATE_TICKET,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
            String to = getContactEmails(conn, ticketID);
            String message =
                "AO Industries support has reactivated Ticket "+ticketID+".\n"+
                "Please browse to \n\n"+
                MasterConfiguration.getTicketURL()+ticketID+"\n\n"+
                "for more details.\n\n"+
                "--------------------------------------------------------------\n"+
                "This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
            ;
            sendEmail(to, "Ticket "+ticketID+" was reactivated.", message, null);
        }
    }

    public static void ticketWork(
        DatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String username,
        String comments
    ) throws IOException, SQLException {
        checkAccessTicket(conn, source, "ticketWork", ticketID);
        if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
        UsernameHandler.checkAccessUsername(conn, source, "ticketWork", username);
        if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to submit Ticket work as user '"+LinuxAccount.MAIL+'\'');

        conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.UNDERWAY, ticketID);

        conn.executeUpdate(
            "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
            ticketID,
            username,
            TicketActionType.WORK_ENTRY,
            comments
        );

        // Notify all clients of the update
        String accounting=getBusinessForTicket(conn, ticketID);
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.TICKETS,
            accounting,
            InvalidateList.allServers,
            false
        );
        invalidateList.addTable(
            conn,
            SchemaTable.TableID.ACTIONS,
            accounting,
            InvalidateList.allServers,
            false
        );

        if(accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())) {
            String to = getContactEmails(conn, ticketID);
            String message =
                "AO Industries support has updated Ticket "+ticketID+".\n"+
                "Please browse to \n\n"+
                MasterConfiguration.getTicketURL()+ticketID+"\n\n"+
                "for more details.\n\n"+
                "--------------------------------------------------------------\n"+
                "This is an automagically generated email.  DO NOT REPLY TO THIS MESSAGE."
            ;
            sendEmail(to, "Ticket "+ticketID+" was updated.", message, null);
        }
    }*/
    // </editor-fold>
    // <editor-fold desc="Internal Master Data Access">
    /**
     * A ticket administrator is part of a business that is also a reseller.
     */
    public static boolean isTicketAdmin(DatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        return conn.executeBooleanQuery("select (select accounting from resellers where accounting=?) is not null", UsernameHandler.getBusinessForUsername(conn, source.getUsername()));
    }

    public static String getBusinessForTicket(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select accounting from tickets where pkey=?", pkey);
    }

    public static String getBrandForTicket(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select brand from tickets where pkey=?", pkey);
    }

    public static String getStatusForTicket(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select status from tickets where pkey=?", pkey);
    }

    public static String getTicketTypeForTicket(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select ticket_type from tickets where pkey=?", pkey);
    }

    public static String getResellerForTicket(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select reseller from tickets where pkey=?", pkey);
    }

    public static String getBusinessForAction(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select ti.accounting from ticket_actions ac, tickets ti where ac.pkey=? and ac.ticket=ti.pkey", pkey);
    }

    public static String getResellerForAction(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeStringQuery("select ti.reseller from ticket_actions ac, tickets ti where ac.pkey=? and ac.ticket=ti.pkey", pkey);
    }

    public static boolean getVisibleAdminOnlyForAction(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        return conn.executeBooleanQuery("select tat.visible_admin_only from ticket_actions ac inner join ticket_action_types tat on ac.action_type=tat.type where ac.pkey=?", pkey);
    }

    /*
    public static boolean isActive(DatabaseConnection conn, int ticketId) throws IOException, SQLException {
        String status=conn.executeStringQuery("select status from tickets where pkey=?", ticketId);
        return
            !TicketStatus.ADMIN_KILL.equals(status)
            && !TicketStatus.CLIENT_KILL.equals(status)
            && !TicketStatus.COMPLETED.equals(status)
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
                    MasterServer.reportError(err, null);
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
                MasterServer.reportError(err, null);
            }
        }
    }

    public static String getContactEmails(DatabaseConnection conn, int pkey) throws IOException, SQLException {
        String contactEmails=conn.executeStringQuery(
            "select contact_emails from tickets where pkey=?",
            pkey
        );
        if(contactEmails.length()==0) return BusinessHandler.getTechnicalEmail(conn, getBusinessForTicket(conn, pkey));
        return contactEmails;
    }

    private static Thread thread;
    
    public static void start() {
        synchronized(System.out) {
            if(thread==null) {
                System.out.print("Starting TicketHandler: ");
                thread=new Thread(new TicketHandler());
                thread.setPriority(Thread.NORM_PRIORITY-1);
                thread.start();
                System.out.println("Done");
            }
        }
    }
     */

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
                        "Creating tickets from IMAP folders",
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
                                            MasterServer.reportWarning(new IOException("Folder does not exist: "+folderName), null);
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
                                                            TicketPriority.NORMAL,
                                                            TicketPriority.NORMAL,
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
                                                            MasterServer.reportWarning(new IOException("Folder does not exist: "+archiveFolderName), null);
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
                            } catch(IOException err) {
                                if(conn.rollbackAndClose()) {
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
