package com.aoindustries.aoserv.master;

/*
 * Copyright 2001-2009 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.email.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import com.oreilly.servlet.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

/**
 * The <code>TicketHandler</code> handles all the accesses to the ticket tables.
 *
 * @author  AO Industries, Inc.
 */
final public class TicketHandler implements Runnable {

    /**
     * The amount of time sleeping between IMAP folder scans.
     */
    public static final long
        SLEEP_INTERVAL= (long)60*1000,
        TIMER_MAX_TIME=(long)15*60*1000,
        TIMER_REMINDER_INTERVAL=(long)60*60*1000
    ;

    /**
     * The email part separator.
     */
    public static final String PART_SEPARATOR="\n----------------------------------------------------------------------------------------------------\n\n";

    public static boolean canAccessAction(MasterDatabaseConnection conn, RequestSource source, int actionId) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "canAccessAction(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(mu.isTicketAdmin()) return true;
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) return false;
                else return true;
            } else {
                return BusinessHandler.canAccessBusiness(conn, source, getBusinessForAction(conn, actionId));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean canAccessTicket(MasterDatabaseConnection conn, RequestSource source, int ticketId) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "canAccessTicket(MasterDatabaseConnection,RequestSource,int)", null);
        try {
            MasterUser mu = MasterServer.getMasterUser(conn, source.getUsername());
            if(mu!=null) {
                if(mu.isTicketAdmin()) return true;
                if(MasterServer.getMasterServers(conn, source.getUsername()).length!=0) return false;
                else return true;
            } else {
                return BusinessHandler.canAccessBusiness(conn, source, getBusinessForTicket(conn, ticketId));
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void checkAccessTicket(MasterDatabaseConnection conn, RequestSource source, String action, int ticketID) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "checkAccessTicket(MasterDatabaseConnection,RequestSource,String,int)", null);
        try {
            if(!canAccessTicket(conn, source, ticketID)) {
                String message=
                    "business_administrator.username="
                    +source.getUsername()
                    +" is not allowed to access ticket: action='"
                    +action
                    +", pkey="
                    +ticketID
                ;
                MasterServer.reportSecurityMessage(source, message);
                throw new SQLException(message);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a ticket with security checks.
     */
    public static int addTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        String accounting,
        String username,
        String type,
        String details,
        long deadline,
        String clientPriority,
        String adminPriority,
        String technology,
        String assignedTo,
        String contactEmails,
        String contactPhoneNumbers
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "addTicket(MasterDatabaseConnection,RequestSource,InvalidateList,String,String,String,String,long,String,String,String,String,String,String)", null);

        try {
            boolean isAdmin=isTicketAdmin(conn, source);
            if(accounting==null) {
                if(!isAdmin) throw new SQLException("Only ticket administrators may create a ticket without a business.");
            } else {
                BusinessHandler.checkAccessBusiness(conn, source, "addTicket", accounting);
                if(BusinessHandler.isBusinessDisabled(conn, accounting)) throw new SQLException("Unable to add Ticket, Business disabled: "+accounting);
            }
            UsernameHandler.checkAccessUsername(conn, source, "addTicket", username);
            if(BusinessHandler.isBusinessAdministratorDisabled(conn, username)) throw new SQLException("Unable to add Ticket, BusinessAdministrator disabled: "+username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to add Ticket for user '"+LinuxAccount.MAIL+'\'');
            if(assignedTo!=null) {
                // Only admin may assign a ticket
                if(!isAdmin) throw new SQLException("Only ticket admin allowed to assign tickets.");
            }
            return addTicket(conn, invalidateList, accounting, username, type, details, deadline, clientPriority, adminPriority, technology, assignedTo, contactEmails, contactPhoneNumbers);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Adds a ticket directly, without any security checks.
     */
    public static int addTicket(
        MasterDatabaseConnection conn,
        InvalidateList invalidateList,
        String accounting,
        String username,
        String type,
        String details,
        long deadline,
        String clientPriority,
        String adminPriority,
        String technology,
        String assignedTo,
        String contactEmails,
        String contactPhoneNumbers
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "addTicket(MasterDatabaseConnection,InvalidateList,String,String,String,String,long,String,String,String,String,String,String)", null);

        try {
            int pkey=conn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('ticket_pkey_seq')");

            if(deadline==Ticket.NO_DEADLINE) {
                conn.executeUpdate(
                    "insert into tickets values(?,?,?,?,?,now(),null,null,null,?,?,?,?,?,?,?)",
                    pkey,
                    accounting,
                    username,
                    type,
                    details,
                    clientPriority,
                    adminPriority,
                    technology,
                    TicketStatus.NEW,
                    assignedTo,
                    contactEmails,
                    contactPhoneNumbers
                );
            } else {
                conn.executeUpdate(
                    "insert into tickets values(?,?,?,?,?,now(),?,null,null,?,?,?,?,?,?,?)",
                    pkey,
                    accounting,
                    username,
                    type,
                    details,
                    new Timestamp(deadline),
                    clientPriority,
                    adminPriority,
                    technology,
                    TicketStatus.NEW,
                    assignedTo,
                    contactEmails,
                    contactPhoneNumbers
                );
            }

            //conn.executeUpdate(
            //    "insert into actions(ticket_id, administrator, time, action_type, comments) values(?,?,now(),?,'AUTO')",
            //    pkey,
            //    username,
            //    ActionType.OPEN_TICKET
            //);

            
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
            }


            // Notify all clients of the updates
            invalidateList.addTable(conn, SchemaTable.TableID.TICKETS, accounting, InvalidateList.allServers, false);
            //invalidateList.addTable(conn, SchemaTable.TableID.ACTIONS, accounting, null);
            return pkey;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void bounceTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "bounceTicket(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
            checkAccessTicket(conn, source, "bounceTicket", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "bounceTicket", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to bounce a Ticket as user '"+LinuxAccount.MAIL+'\'');

            conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.BOUNCED, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
                ticketID,
                username,
                ActionType.BOUNCED,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void changeTicketAdminPriority(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String priority, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "changeTicketAdminPriority(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
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
                ActionType.ADMIN_PRIORITY_CHANGE,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setTicketAssignedTo(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String assignedTo, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "setTicketAssignedTo(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
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
                ActionType.ASSIGNED,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setTicketBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String accounting, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "setTicketBusiness(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "setTicketBusiness", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "setTicketBusiness", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to assign a Ticket as user '"+LinuxAccount.MAIL+'\'');

            if(accounting!=null) BusinessHandler.checkAccessBusiness(conn, source, "setTicketBusiness", accounting);

            String oldValue=conn.executeStringQuery("select accounting from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set accounting=? where pkey=?", accounting, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.SET_BUSINESS,
                oldValue,
                comments
            );

            // Notify all clients of the update
            String oldAccounting=getBusinessForTicket(conn, ticketID);
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.TICKETS,
                oldAccounting,
                InvalidateList.allServers,
                false
            );
            invalidateList.addTable(
                conn,
                SchemaTable.TableID.ACTIONS,
                oldAccounting,
                InvalidateList.allServers,
                false
            );
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setTicketContactEmails(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String contactEmails, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "setTicketContactEmails(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "setTicketContactEmails", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "setTicketContactEmails", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to set a Ticket's contact emails as user '"+LinuxAccount.MAIL+'\'');

            String oldValue=conn.executeStringQuery("select contact_emails from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set contact_emails=? where pkey=?", contactEmails, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.SET_CONTACT_EMAILS,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void setTicketContactPhoneNumbers(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String contactPhoneNumbers, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "setTicketContactPhoneNumbers(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "setTicketContactPhoneNumbers", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "setTicketContactPhoneNumbers", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change a Ticket's contact phone numbers as user '"+LinuxAccount.MAIL+'\'');

            String oldValue=conn.executeStringQuery("select contact_phone_numbers from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set contact_phone_numbers=? where pkey=?", contactPhoneNumbers, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.SET_CONTACT_PHONES,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void changeTicketClientPriority(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID, 
        String priority, 
        String username, 
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "changeTicketClientPriority(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "changeTicketClientPriority", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "changeTicketClientPriority", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change Ticket client priority as user  '"+LinuxAccount.MAIL+'\'');

            String oldValue=conn.executeStringQuery("select client_priority from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set client_priority=? where pkey=?", priority, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.CLIENT_PRIORITY_CHANGE,
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

            if(
                priority.equals(TicketPriority.URGENT)
                && accounting!=null && !accounting.equals(BusinessHandler.getRootBusiness())
            ) {
                String details = conn.executeStringQuery("select details from tickets where pkey=?", ticketID); 
                String message = details+"\n\n"+
                    MasterConfiguration.getTicketURL()+ticketID+"\n\n"
                ;
                sendEmail("support@aoindustries.com", "URGENT ticket notification", message, "ID: "+ticketID+" "+details);
            }

        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void changeTicketDeadline(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID, 
        long deadline, 
        String username, 
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "changeTicketDeadline(MasterDatabaseConnection,RequestSource,InvalidateList,int,long,String,String)", null);
        try {
            checkAccessTicket(conn, source, "changeTicketDeadline", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "changeTicketDeadline", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change Ticket client priority as user  '"+LinuxAccount.MAIL+'\'');

            if(!isTicketAdmin(conn, source)) throw new SQLException("Only ticket administrators may change the deadline.");

            String oldValue=conn.executeStringQuery("select deadline from tickets where pkey=?", ticketID);

            if(deadline==Ticket.NO_DEADLINE) {
                conn.executeUpdate(
                    "update tickets set deadline=null where pkey=?",
                    ticketID
                );
            } else {
                conn.executeUpdate(
                    "update tickets set deadline=? where pkey=?",
                    new Timestamp(deadline),
                    ticketID
                );
            }

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.DEADLINE_CHANGE,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void changeTicketTechnology(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID,
        String technology,
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "changeTicketTechnology(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "changeTicketTechnology", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "changeTicketTechnology", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change Ticket client priority as user  '"+LinuxAccount.MAIL+'\'');

            String oldValue=conn.executeStringQuery("select technology from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set technology=? where pkey=?", technology, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.TECHNOLOGY_CHANGE,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void changeTicketType(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID, 
        String type,
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "changeTicketType(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String,String)", null);
        try {
            checkAccessTicket(conn, source, "changeTicketType", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "changeTicketType", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to change Ticket type as user '"+LinuxAccount.MAIL+'\'');
            String oldValue=conn.executeStringQuery("select ticket_type from tickets where pkey=?", ticketID);

            conn.executeUpdate("update tickets set ticket_type=? where pkey=?", type, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, old_value, comments) values(?,?,?,?,?)",
                ticketID,
                username,
                ActionType.TYPE_CHANGE,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void completeTicket(
        MasterDatabaseConnection conn,
        RequestSource source, 
        InvalidateList invalidateList,
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "completeTicket(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
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
                ActionType.COMPLETE_TICKET,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all actions for one ticket.
     */
    public static void getActionsTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        int ticketID
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getActionsTicket(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,int)", null);
        try {
            if(canAccessTicket(conn, source, ticketID)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Action(),
                    "select\n"
                    + "  *\n"
                    + "from\n"
                    + "  actions\n"
                    + "where\n"
                    + "  ticket_id=?",
                    ticketID
                );
            } else {
                List<Action> emptyList = Collections.emptyList();
                MasterServer.writeObjects(source, out, provideProgress, emptyList);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all actions for one business administrator.
     */
    public static void getActionsBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getActionsBusinessAdministrator(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            UsernameHandler.checkAccessUsername(conn, source, "getActionsBusinessAdministrator", username);

            MasterServer.writeObjects(
                conn,
                source,
                out,
                provideProgress,
                new Action(),
                "select\n"
                + "  *\n"
                + "from\n"
                + "  actions\n"
                + "where\n"
                + "  administrator=?",
                username
            );
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all tickets created by one person.
     */
    public static void getTicketsBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getTicketsBusinessAdministrator(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            if(isTicketAdmin(conn, source)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Ticket(),
                    "select * from tickets where created_by=?",
                    username,
                    source.getUsername()
                );
            } else {
                if(UsernameHandler.canAccessUsername(conn, source, username)) {
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Ticket(),
                        "select * from tickets where administrator=?",
                        username
                    );
                } else {
                    List<Ticket> emptyList = Collections.emptyList();
                    MasterServer.writeObjects(source, out, provideProgress, emptyList);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all tickets that one non-admin user may see.
     */
    public static void getTicketsCreatedBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getTicketsCreatedBusinessAdministrator(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            if(UsernameHandler.canAccessUsername(conn, source, username)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Ticket(),
                    "select * from tickets where created_by=?",
                    username
                );
            } else {
                List<Ticket> emptyList = Collections.emptyList();
                MasterServer.writeObjects(source, out, provideProgress, emptyList);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all tickets that one non-admin user may see.
     */
    public static void getTicketsClosedBusinessAdministrator(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String username
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getTicketsClosedBusinessAdministrator(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            if(UsernameHandler.canAccessUsername(conn, source, username)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Ticket(),
                    "select * from tickets where closed_by=?",
                    username
                );
            } else {
                List<Ticket> emptyList = Collections.emptyList();
                MasterServer.writeObjects(source, out, provideProgress, emptyList);
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Gets all tickets for one business
     */
    public static void getTicketsBusiness(
        MasterDatabaseConnection conn,
        RequestSource source,
        CompressedDataOutputStream out,
        boolean provideProgress,
        String accounting
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getTicketsBusiness(MasterDatabaseConnection,RequestSource,CompressedDataOutputStream,boolean,String)", null);
        try {
            if(isTicketAdmin(conn, source)) {
                MasterServer.writeObjects(
                    conn,
                    source,
                    out,
                    provideProgress,
                    new Ticket(),
                    "select * from tickets where accounting=?",
                    accounting,
                    source.getUsername()
                );
            } else {
                if(BusinessHandler.canAccessBusiness(conn, source, accounting)) {
                    MasterServer.writeObjects(
                        conn,
                        source,
                        out,
                        provideProgress,
                        new Ticket(),
                        "select * from tickets where accounting=?",
                        accounting
                    );
                } else {
                    List<Ticket> emptyList = Collections.emptyList();
                    MasterServer.writeObjects(source, out, provideProgress, emptyList);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void holdTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "holdTicket(MasterDatabaseConnection,RequestSource,InvalidateList,int,String)", null);
        try {
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
                isAdminChange?ActionType.ADMIN_HOLD:ActionType.CLIENT_HOLD,
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

        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void killTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "killTicket(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
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
                isClientChange?ActionType.CLIENT_KILLED:ActionType.ADMIN_KILL,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void reactivateTicket(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID,
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "reactivateTicket(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
            checkAccessTicket(conn, source, "holdTicket", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "holdTicket", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to reactivate Ticket as user '"+LinuxAccount.MAIL+'\'');

            conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.UNDERWAY, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
                ticketID,
                username,
                ActionType.REACTIVATE_TICKET,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static void ticketWork(
        MasterDatabaseConnection conn,
        RequestSource source,
        InvalidateList invalidateList,
        int ticketID, 
        String username,
        String comments
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "ticketWork(MasterDatabaseConnection,RequestSource,InvalidateList,int,String,String)", null);
        try {
            checkAccessTicket(conn, source, "ticketWork", ticketID);
            if(!isActive(conn, ticketID)) throw new SQLException("Ticket no longer active: "+ticketID);
            UsernameHandler.checkAccessUsername(conn, source, "ticketWork", username);
            if(username.equals(LinuxAccount.MAIL)) throw new SQLException("Not allowed to submit Ticket work as user '"+LinuxAccount.MAIL+'\'');

            conn.executeUpdate("update tickets set status=? where pkey=?", TicketStatus.UNDERWAY, ticketID);

            conn.executeUpdate(
                "insert into actions(ticket_id, administrator, action_type, comments) values(?,?,?,?)",
                ticketID,
                username,
                ActionType.WORK_ENTRY,
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Admin access to named info is granted if either no server is restricted, or one of the
     * granted servers is a named machine.
     */
    public static boolean isTicketAdmin(MasterDatabaseConnection conn, RequestSource source) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, TicketHandler.class, "isTicketAdmin(MasterDatabaseConnection,RequestSource)", null);
        try {
            MasterUser mu=MasterServer.getMasterUser(conn, source.getUsername());
            return mu!=null && mu.isTicketAdmin();
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static String getBusinessForTicket(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getBusinessForTicket(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select accounting from tickets where pkey=?", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static String getBusinessForAction(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getBusinessForAction(MasterDatabaseConnection,int)", null);
        try {
            return conn.executeStringQuery("select ti.accounting from actions ac, tickets ti where ac.pkey=? and ac.ticket_id=ti.pkey", pkey);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public static boolean isActive(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "isActive(MasterDatabaseConnection,int)", null);
        try {
            String status=conn.executeStringQuery("select status from tickets where pkey=?", pkey);
            return
                !TicketStatus.ADMIN_KILL.equals(status)
                && !TicketStatus.CLIENT_KILL.equals(status)
                && !TicketStatus.COMPLETED.equals(status)
            ;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

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
    
    public static String getContactEmails(MasterDatabaseConnection conn, int pkey) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getContactEmails(MasterDatabaseConnection,int)", null);
        try {
            String contactEmails=conn.executeStringQuery(
                "select contact_emails from tickets where pkey=?",
                pkey
            );
            if(contactEmails.length()==0) return BusinessHandler.getTechnicalEmail(conn, getBusinessForTicket(conn, pkey));
            return contactEmails;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
    
    private static Thread thread;
    
    public static void start() {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "start()", null);
        try {
            synchronized(System.out) {
                if(thread==null) {
                    System.out.print("Starting TicketHandler: ");
                    thread=new Thread(new TicketHandler());
                    thread.setPriority(Thread.NORM_PRIORITY-1);
                    thread.start();
                    System.out.println("Done");
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    public void run() {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "run()", null);
        try {
            while(true) {
                try {
                    while(true) {
                        try {
                            Thread.sleep(SLEEP_INTERVAL);
                        } catch(InterruptedException err) {
                            MasterServer.reportWarning(err, null);
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
                            MasterDatabaseConnection conn=(MasterDatabaseConnection)MasterDatabase.getDatabase().createDatabaseConnection();
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
                                                                Ticket.NO_DEADLINE,
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
                    MasterServer.reportError(T, null);
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private static final String[] MATCH_HEADERS=new String[]{"Subject","Date","From","Cc"};
    
    /**
     * Gets the String form of the message body.
     */
    public String getMessageBody(Message message) throws IOException, MessagingException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getMessageBody(Message)", null);
        try {
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
                MasterServer.reportWarning(err, null);
                return message.getContent().toString();
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    private void getMessageBody0(Object content, StringBuilder SB) throws IOException, MessagingException {
        Profiler.startProfile(Profiler.UNKNOWN, TicketHandler.class, "getMessageBody0(Object,StringBuilder)", null);
        try {
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
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}
