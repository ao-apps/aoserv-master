/*
 * Copyright 2009-2013 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master;

import com.aoindustries.aoserv.client.Language;
import com.aoindustries.aoserv.client.TicketPriority;
import com.aoindustries.aoserv.client.TicketStatus;
import com.aoindustries.aoserv.client.TicketType;
import com.aoindustries.aoserv.client.validator.AccountingCode;
import com.aoindustries.sql.DatabaseConnection;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.logging.QueuedHandler;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * The same as <code>com.aoindustries.aoserv.client.TicketLoggingHandler</code>
 * except with direct database access.
 * 
 * @author  AO Industries, Inc.
 */
final public class TicketLoggingHandler extends QueuedHandler {

    private static final List<TicketLoggingHandler> handlers = new ArrayList<TicketLoggingHandler>();

    /**
     * Only one TicketLoggingHandler will be created per unique summaryPrefix and category.
     */
    public static Handler getHandler(String summaryPrefix, int category) {
        synchronized(handlers) {
            for(TicketLoggingHandler handler : handlers) {
                if(
                    handler.summaryPrefix.equals(summaryPrefix)
                    && handler.category==category
                ) return handler;
            }
            TicketLoggingHandler handler = new TicketLoggingHandler(summaryPrefix, category);
            handlers.add(handler);
            return handler;
        }
    }

    private final String summaryPrefix;
    private final int category;

    private TicketLoggingHandler(String summaryPrefix, int category) {
        super(
            "Console logger for "+summaryPrefix,
            "Ticket logger for "+summaryPrefix
        );
        this.summaryPrefix = summaryPrefix;
        this.category = category;
    }

    protected boolean useCustomLogging(LogRecord record) {
        return record.getLevel().intValue()>Level.FINE.intValue();
    }

    protected void doCustomLogging(Formatter formatter, LogRecord record, String fullReport) {
        try {
            AccountingCode rootAccounting = BusinessHandler.getRootBusiness();
            Level level = record.getLevel();
            // Generate the summary from level, prefix classname, method
            StringBuilder tempSB = new StringBuilder();
            tempSB.append('[').append(level).append(']');
            if(summaryPrefix!=null && summaryPrefix.length()>0) tempSB.append(' ').append(summaryPrefix);
            tempSB.append(" - ").append(record.getSourceClassName()).append(" - ").append(record.getSourceMethodName());
            String summary = tempSB.toString();

            // Start the transaction
            InvalidateList invalidateList=new InvalidateList();
            DatabaseConnection conn=MasterDatabase.getDatabase().createDatabaseConnection();
            try {
                boolean connRolledBack=false;
                try {
                    // Look for an existing ticket to append
                    int existingTicket = conn.executeIntQuery(
                        "select\n"
                        + "  coalesce(\n"
                        + "    (\n"
                        + "      select\n"
                        + "        pkey\n"
                        + "      from\n"
                        + "        tickets\n"
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
                        + "        pkey desc\n"
                        + "      limit 1\n"
                        + "    ), -1\n"
                        + "  )",
                        TicketStatus.OPEN,
                        TicketStatus.HOLD,
                        TicketStatus.BOUNCED,
                        rootAccounting,
                        rootAccounting,
                        Language.EN,
                        TicketType.LOGS,
                        summary,
                        category
                    );
                    if(existingTicket!=-1) {
                        TicketHandler.addTicketAnnotation(
                            conn,
                            invalidateList,
                            existingTicket,
                            null,
                            com.aoindustries.aoserv.client.TicketLoggingHandler.generateActionSummary(formatter, record),
                            fullReport
                        );
                    } else {
                        // The priority depends on the log level
                        String priorityName;
                        int intLevel = level.intValue();
                        if(intLevel<=Level.CONFIG.intValue()) priorityName = TicketPriority.LOW;           // FINE < level <= CONFIG
                        else if(intLevel<=Level.INFO.intValue()) priorityName = TicketPriority.NORMAL;     // CONFIG < level <=INFO
                        else if(intLevel<=Level.WARNING.intValue()) priorityName = TicketPriority.HIGH;    // INFO < level <=WARNING
                        else priorityName = TicketPriority.URGENT;                                         // WARNING < level
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
                            TicketStatus.OPEN,
                            -1,
                            "",
                            "",
                            ""
                        );
                    }
                } catch(RuntimeException err) {
                    if(conn.rollback()) {
                        connRolledBack=true;
                        invalidateList=null;
                    }
                    throw err;
                } catch(IOException err) {
                    if(conn.rollback()) {
                        connRolledBack=true;
                        invalidateList=null;
                    }
                    throw err;
                } catch(SQLException err) {
                    if(conn.rollbackAndClose()) {
                        connRolledBack=true;
                        invalidateList=null;
                    }
                    throw err;
                } finally {
                    if(!connRolledBack && !conn.isClosed()) conn.commit();
                }
            } finally {
                conn.releaseConnection();
            }
            if(invalidateList!=null) MasterServer.invalidateTables(invalidateList, null);
        } catch(Exception err) {
            ErrorPrinter.printStackTraces(err);
        }
    }
}
