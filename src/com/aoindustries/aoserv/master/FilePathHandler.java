package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2006 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import com.aoindustries.aoserv.client.*;
import com.aoindustries.io.*;
import com.aoindustries.profiler.*;
import com.aoindustries.sql.*;
import com.aoindustries.util.*;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * The <code>FilePathHandler</code> handles all the accesses to the <code>file_paths</code> table.
 *
 * @author  AO Industries, Inc.
 */
final public class FilePathHandler {

    private static final Object filePathsPKeySeqLock=new Object();

    /**
     * Resulves a group of parents.
     */
    public static int[] getParents(
        BackupDatabaseConnection backupConn,
        int[] file_paths,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "getParents(BackupDatabaseConnection,int[],int)", null);
        try {
            // Build the hash of indexes
            Map<Integer,Integer> filePathIndexes=new HashMap<Integer,Integer>(batchSize);
            for(int c=0;c<batchSize;c++) {
                if(filePathIndexes.put(file_paths[c], c)!=null) throw new SQLException("file_path found more than once in call to getParents: "+file_paths[c]);
            }
            return getParents(backupConn, file_paths, batchSize, filePathIndexes, new StringBuilder());
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Resolves a group of parents.
     */
    public static int[] getParents(
        BackupDatabaseConnection backupConn,
        int[] file_paths,
        int batchSize,
        Map<Integer,Integer> filePathIndexes,
        StringBuilder SB
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "getParents(BackupDatabaseConnection,int[],int,Map<Integer,Integer>)", null);
        try {
            SB.setLength(0);
            SB.append(
                "select\n"
                + "  pkey,\n"
                + "  parent\n"
                + "from\n"
                + "  file_paths\n"
                + "where\n"
                + "  pkey in (");
            for(int c=0;c<batchSize;c++) {
                if(c>0) SB.append(',');
                SB.append(file_paths[c]);
            }
            SB.append(')');

            int[] parents=new int[batchSize];

            String sqlString=null;
            Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
            try {
                backupConn.incrementQueryCount();
                ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                try {
                    while(results.next()) {
                        int pkey=results.getInt(1);
                        int parent=results.getInt(2);
                        int index=filePathIndexes.get(pkey).intValue();
                        parents[index]=parent;
                    }
                } finally {
                    results.close();
                }
            } catch(SQLException err) {
                System.err.println("Error from query: "+sqlString);
                throw err;
            } finally {
                stmt.close();
            }
            
            return parents;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Finds or adds a single path.
     */
    public static int findOrAddFilePath(
        BackupDatabaseConnection backupConn,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "findOrAddFilePath(BackupDatabaseConnection,String)", null);
        try {
            return findOrAddFilePaths(backupConn, new String[] {path}, 1)[0];
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Finds or adds multiple paths.  Any transaction on the connection is commited at the start of this method.
     * TODO: This can use the path column for faster queries
     */
    public static int[] findOrAddFilePaths(
        BackupDatabaseConnection backupConn,
        String[] paths,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "findOrAddFilePaths(BackupDatabaseConnection,String[],int)", null);
        try {
            // The return values go here
            int[] filePaths=new int[batchSize];
            if(batchSize==0) return filePaths;

            // Make a copy of paths for later processing
            String[] pathCopies=new String[batchSize];
            String[] workingFilenames=new String[batchSize];
            int[] parents=new int[batchSize];

            // Duplicates are avoided during each query
            int[] selectParents=new int[batchSize];
            String[] selectFilenames=new String[batchSize];
            String[] selectPaths=new String[batchSize];

            // Reusable StringBuilder, call setLength(0) before using
            StringBuilder SB=new StringBuilder();

            // Retry the query if the transaction is aborted
            int attemptCount=0;
            while(true) {
                attemptCount++;

                try {
                    // First pass, set initial parent and filePaths if is a root node
                    for(int c=0;c<batchSize;c++) {
                        String path=paths[c];
                        if(path.length()==0) throw new SQLException("Invalid path, must not be empty string.");
                        int pathLen=path.length();
                        int slashPos=path.indexOf('/');
                        if(slashPos==pathLen-1) {
                            // Root node
                            int rootPKey=getRootNode(backupConn, path);
                            if(rootPKey==-1) throw new SQLException("Invalid path, only a root node may end with '/': "+path);
                            filePaths[c]=rootPKey;
                            pathCopies[c]=null;
                            parents[c]=0;
                        } else {
                            // Parse out the root node
                            int rootPKey=getRootNode(backupConn, path.substring(0, slashPos+1));
                            if(rootPKey==-1) throw new SQLException("Unable to find root for path: "+path);
                            filePaths[c]=-1;
                            pathCopies[c]=path.substring(slashPos+1);
                            parents[c]=rootPKey;
                        }
                        workingFilenames[c]=null;
                    }

                    while(true) {
                        // Generate the SQL to lookup existing entries
                        int selectSize=0;
                        SB.setLength(0);
                        SB.append(
                            "select\n"
                            + "  pkey,\n"
                            + "  parent,\n"
                            + "  filename\n"
                            + "from\n"
                            + "  file_paths\n"
                            + "where\n");
                        for(int c=0;c<batchSize;c++) {
                            if(filePaths[c]==-1) {
                                int parent=parents[c];
                                String path=pathCopies[c];
                                int pos=path.indexOf('/');
                                String filename=workingFilenames[c]=pos==-1?path:path.substring(0, pos);
                                if(filename.length()==0) throw new SQLException("Invalid path, found empty filename: "+paths[c]);

                                // Look for a match already in this select
                                boolean found=false;
                                for(int d=0;d<selectSize;d++) {
                                    if(
                                        selectParents[d]==parent
                                        && selectFilenames[d].equals(filename)
                                    ) {
                                        found=true;
                                        break;
                                    }
                                }
                                if(!found) {
                                    selectParents[selectSize]=parent;
                                    selectFilenames[selectSize]=filename;
                                    selectPaths[selectSize]=paths[c];
                                    SB.append(selectSize==0?"  (parent=":"  or (parent=").append(parent).append(" and filename=?)\n");
                                    selectSize++;
                                }
                            }
                        }

                        // This flag will stay true if all paths are done
                        boolean allPathsDone=true;

                        // Lookup existing, flagging if found by nulling out the index in selectFilenames
                        PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(SB.toString());
                        try {
                            for(int c=0;c<selectSize;c++) {
                                pstmt.setString(c+1, SQLUtility.encodeString(selectFilenames[c]));
                            }
                            ResultSet results=pstmt.executeQuery();
                            try {
                                while(results.next()) {
                                    int pkey=results.getInt(1);
                                    int parent=results.getInt(2);
                                    String filename=SQLUtility.decodeString(results.getString(3));

                                    // Flag as not needing inserted
                                    for(int c=0;c<selectSize;c++) {
                                        if(
                                            parent==selectParents[c]
                                            && filename.equals(selectFilenames[c])
                                        ) {
                                            selectFilenames[c]=null;
                                            break;
                                        }
                                    }

                                    // Get all that were found ready for the next pass
                                    for(int c=0;c<batchSize;c++) {
                                        if(
                                            filePaths[c]==-1
                                            && parent==parents[c]
                                            && filename.equals(workingFilenames[c])
                                        ) {
                                            int filenameLen=filename.length();
                                            if(filenameLen==pathCopies[c].length()) {
                                                // The full path is done
                                                filePaths[c]=pkey;
                                            } else {
                                                parents[c]=pkey;
                                                pathCopies[c]=pathCopies[c].substring(filenameLen+1);
                                                allPathsDone=false;
                                            }
                                        }
                                    }
                                }
                            } finally {
                                results.close();
                            }
                        } catch(SQLException err) {
                            System.err.println("Error from query: "+pstmt.toString());
                            throw err;
                        } finally {
                            pstmt.close();
                        }

                        // Add any missing
                        int needAddedCount=0;
                        for(int c=0;c<selectSize;c++) {
                            if(selectFilenames[c]!=null) needAddedCount++;
                        }
                        if(needAddedCount>0) {
                            // Get the pkeys in a single query
                            int startPKey;
                            synchronized(filePathsPKeySeqLock) {
                                startPKey=backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select nextval('file_paths_pkey_seq')");
                                if(needAddedCount>1) {
                                    backupConn.executeIntQuery(Connection.TRANSACTION_READ_COMMITTED, false, true, "select setval('file_paths_pkey_seq', ?)", startPKey+needAddedCount-1);
                                }
                            }

                            // Insert all of the rows
                            pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, false).prepareStatement("insert into file_paths values(?,?,?,?);\n");
                            try {
                                for(int c=0;c<selectSize;c++) {
                                    String filename=selectFilenames[c];
                                    if(filename!=null) {
                                        int parent=selectParents[c];

                                        // Add the row
                                        pstmt.setInt(1, startPKey);
                                        pstmt.setInt(2, parent);
                                        pstmt.setString(3, SQLUtility.encodeString(filename));
                                        pstmt.setString(4, selectPaths[c]);
                                        pstmt.addBatch();

                                        // Get it ready for the next pass
                                        for(int d=0;d<batchSize;d++) {
                                            if(
                                                filePaths[d]==-1
                                                && parent==parents[d]
                                                && filename.equals(workingFilenames[d])
                                            ) {
                                                int filenameLen=filename.length();
                                                if(filenameLen==pathCopies[d].length()) {
                                                    // The full path is done
                                                    filePaths[d]=startPKey;
                                                } else {
                                                    parents[d]=startPKey;
                                                    pathCopies[d]=pathCopies[d].substring(filenameLen+1);
                                                    allPathsDone=false;
                                                }
                                            }
                                        }
                                        startPKey++;
                                    }
                                }
                                pstmt.executeBatch();
                            } catch(SQLException err) {
                                System.err.println("Error from query: "+pstmt.toString());
                                throw err;
                            } finally {
                                pstmt.close();
                            }
                        }

                        // Return if all done
                        if(allPathsDone) break;;
                    }

                    // Return the results
                    return filePaths;
                } catch(SQLException err) {
                    if(
                        attemptCount<BackupDatabaseConnection.MAX_ABORTED_RETRIES
                        && BackupDatabaseConnection.isTransactionAbort(err)
                    ) {
                        MasterServer.reportWarning(err, new Object[] {"attemptCount="+attemptCount, "BackupDatabaseConnection.MAX_ABORTED_RETRIES="+BackupDatabaseConnection.MAX_ABORTED_RETRIES});

                        // Rollback any changes
                        backupConn.rollbackAndClose();
                        
                        // Release the connection, causing a fresh one to be allocated on next use
                        backupConn.releaseConnection();
                        
                        // Delay before next attempt
                        try {
                            Thread.sleep(BackupDatabaseConnection.ABORTED_RETRY_DELAY);
                        } catch(InterruptedException err2) {
                            MasterServer.reportWarning(err2, null);
                        }
                    } else throw err;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Finds a root node.
     *
     * @param  path  the path with trailing slash, such as / or A:/
     *
     * @return  the pkey that was found or -1 if not found
     */
    private static final Object rootCacheLock=new Object();
    private static Map<String,Integer> rootCache;
    private static int[] rootPKeys;
    private static void loadRootNodes(
        BackupDatabaseConnection backupConn
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, FilePathHandler.class, "loadRootNodes(BackupDatabaseConnection)", null);
        try {
            synchronized(rootCacheLock) {
                if(rootCache==null) {
                    Map<String,Integer> tempHash=new HashMap<String,Integer>();
                    String sqlString="select pkey, filename||'/' from file_paths where pkey=parent";
                    Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                    try {
                        backupConn.incrementQueryCount();
                        ResultSet results=stmt.executeQuery(sqlString);
                        try {
                            while(results.next()) {
                                int pkey=results.getInt(1);
                                String filename=results.getString(2);
                                tempHash.put(filename, pkey);
                            }
                        } finally {
                            results.close();
                        }
                    } catch(SQLException err) {
                        System.err.println("Error from query: "+sqlString);
                        throw err;
                    } finally {
                        stmt.close();
                    }
                    int[] tempPKeys=new int[tempHash.size()];
                    Iterator<String> pkeys=tempHash.keySet().iterator();
                    int pos=0;
                    while(pkeys.hasNext()) {
                        String path=(String)pkeys.next();
                        tempPKeys[pos++]=tempHash.get(path);
                    }
                    rootCache=tempHash;
                    rootPKeys=tempPKeys;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int getRootNode(
        BackupDatabaseConnection backupConn,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, FilePathHandler.class, "getRootNode(BackupDatabaseConnection,String)", null);
        try {
            synchronized(rootCacheLock) {
                loadRootNodes(backupConn);
                Integer O=rootCache.get(path);
                return O==null?-1:O.intValue();
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    public static int[] getRootNodes(
        BackupDatabaseConnection backupConn
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.FAST, FilePathHandler.class, "getRootNodes(BackupDatabaseConnection)", null);
        try {
            synchronized(rootCacheLock) {
                loadRootNodes(backupConn);
                return rootPKeys;
            }
        } finally {
            Profiler.endProfile(Profiler.FAST);
        }
    }

    /**
     * Finds a single path.
     */
    public static int findFilePath(
        BackupDatabaseConnection backupConn,
        String path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "findFilePath(BackupDatabaseConnection,String)", null);
        try {
            return findFilePaths(backupConn, new String[] {path}, 1)[0];
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Finds multiple paths.
     * TODO: This can use the path column for faster queries
     */
    public static int[] findFilePaths(
        BackupDatabaseConnection backupConn,
        String[] paths,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "findFilePaths(BackupDatabaseConnection,String[],int)", null);
        try {
            // The return values go here
            int[] filePaths=new int[batchSize];
            if(batchSize==0) return filePaths;
            Arrays.fill(filePaths, -2);

            // Make a copy of paths for laster processing
            String[] pathCopies=new String[batchSize];
            String[] workingFilenames=new String[batchSize];
            int[] parents=new int[batchSize];

            // First pass, set initial parent to 0 and filePaths to 0 if is '/'
            for(int c=0;c<batchSize;c++) {
                String path=paths[c];
                if(path.length()==0) throw new SQLException("Invalid path, must not be empty string.");
                int pathLen=path.length();
                int slashPos=path.indexOf('/');
                if(slashPos==pathLen-1) {
                    // Root node
                    int rootPKey=getRootNode(backupConn, path);
                    if(rootPKey==-1) throw new SQLException("Invalid path, only a root node may end with '/': "+path);
                    filePaths[c]=rootPKey;
                } else {
                    // Parse out the root node
                    int rootPKey=getRootNode(backupConn, path.substring(0, slashPos+1));
                    if(rootPKey==-1) throw new SQLException("Unable to find root for path: "+path);
                    pathCopies[c]=path.substring(slashPos+1);
                    parents[c]=rootPKey;
                }
            }

            // Duplicates are avoided during each query
            int[] selectParents=new int[batchSize];
            String[] selectFilenames=new String[batchSize];
            StringBuilder SB=new StringBuilder();
            while(true) {
                // Generate the SQL to lookup existing entries
                int selectSize=0;
                SB.setLength(0);
                SB.append(
                    "select\n"
                    + "  pkey,\n"
                    + "  parent,\n"
                    + "  filename\n"
                    + "from\n"
                    + "  file_paths\n"
                    + "where\n");
                for(int c=0;c<batchSize;c++) {
                    if(filePaths[c]==-2) {
                        int parent=parents[c];
                        String path=pathCopies[c];
                        int pos=path.indexOf('/');
                        String filename=workingFilenames[c]=pos==-1?path:path.substring(0, pos);
                        if(filename.length()==0) throw new SQLException("Invalid path, found empty filename: "+paths[c]);

                        // Look for a match already in this select
                        boolean found=false;
                        for(int d=0;d<selectSize;d++) {
                            if(
                                selectParents[d]==parent
                                && selectFilenames[d].equals(filename)
                            ) {
                                found=true;
                                break;
                            }
                        }
                        if(!found) {
                            selectParents[selectSize]=parent;
                            selectFilenames[selectSize]=filename;
                            SB.append(selectSize==0?"  (parent=":"  or (parent=").append(parent).append(" and filename=?)\n");
                            selectSize++;
                        }
                    }
                }

                // This flag will stay true if all paths are done
                boolean allPathsDone=true;

                if(selectSize>0) {
                    // Lookup existing, flagging if found by nulling out the index in selectFilenames
                    PreparedStatement pstmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).prepareStatement(SB.toString());
                    try {
                        for(int c=0;c<selectSize;c++) {
                            pstmt.setString(c+1, SQLUtility.encodeString(selectFilenames[c]));
                        }
                        ResultSet results=pstmt.executeQuery();
                        try {
                            while(results.next()) {
                                int pkey=results.getInt(1);
                                int parent=results.getInt(2);
                                String filename=SQLUtility.decodeString(results.getString(3));

                                // Flag as not needing inserted
                                for(int c=0;c<selectSize;c++) {
                                    if(
                                        parent==selectParents[c]
                                        && filename.equals(selectFilenames[c])
                                    ) {
                                        selectFilenames[c]=null;
                                        break;
                                    }
                                }

                                // Get all that were found ready for the next pass
                                for(int c=0;c<batchSize;c++) {
                                    if(
                                        filePaths[c]==-2
                                        && parent==parents[c]
                                        && filename.equals(workingFilenames[c])
                                    ) {
                                        int filenameLen=filename.length();
                                        if(filenameLen==pathCopies[c].length()) {
                                            // The full path is done
                                            filePaths[c]=pkey;
                                        } else {
                                            parents[c]=pkey;
                                            pathCopies[c]=pathCopies[c].substring(filenameLen+1);
                                            allPathsDone=false;
                                        }
                                    }
                                }
                            }
                        } finally {
                            results.close();
                        }
                    } catch(SQLException err) {
                        System.err.println("Error from query: "+pstmt.toString());
                        throw err;
                    } finally {
                        pstmt.close();
                    }

                    // Flag those that are missing as not found
                    for(int c=0;c<selectSize;c++) {
                        String filename=selectFilenames[c];
                        if(filename!=null) {
                            // Make sure those that were not found are not looked for next pass
                            int parent=selectParents[c];
                            for(int d=0;d<batchSize;d++) {
                                if(
                                    filePaths[d]==-2
                                    && parent==parents[d]
                                    && filename.equals(workingFilenames[d])
                                ) {
                                    filePaths[d]=-1;
                                }
                            }
                        }
                    }
                }

                // Return if all done
                if(allPathsDone) break;;
            }

            // Return the results
            return filePaths;
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes multiple file paths if they are not referenced by file_backups anymore.
     */
    public static void removeUnusedFilePaths(
        BackupDatabaseConnection backupConn,
        int[] file_paths,
        int batchSize
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "removeUnusedFilePaths(BackupDatabaseConnection,int[],int)", null);
        try {
            int[] currentPaths=new int[batchSize];
            System.arraycopy(file_paths, 0, currentPaths, 0, batchSize);
            int[] currentParents=new int[batchSize];
            int currentBatchSize=batchSize;
            StringBuilder SB=new StringBuilder();
            while(currentBatchSize>0) {
                // Get the pkey and parent of any unused file paths
                SB.setLength(0);
                SB.append(
                    "select\n"
                    + "  fp1.pkey,\n"
                    + "  fp1.parent\n"
                    + "from\n"
                    + "  file_paths fp1\n"
                    + "where\n"
                    + "  fp1.pkey in (");
                boolean didOne=false;
                for(int c=0;c<currentBatchSize;c++) {
                    if(didOne) SB.append(',');
                    else didOne=true;
                    SB.append(currentPaths[c]);
                }
                if(!didOne) return;
                SB.append(
                    ")\n"
                    + "  and fp1.pkey!=fp1.parent\n"
                    + "  and (select fp2.pkey from file_paths fp2 where fp1.pkey=fp2.parent limit 1) is null\n"
                    + "  and (select fb.pkey from file_backups fb where fp1.pkey=fb.file_path limit 1) is null\n"
                    + "  and (select fbr1.pkey from file_backup_roots fbr1 where fp1.pkey=fbr1.file_path limit 1) is null\n"
                    + "  and (select fbr2.pkey from file_backup_roots fbr2 where fp1.pkey=fbr2.parent limit 1) is null");
                String sqlString=null;
                Statement stmt=backupConn.getConnection(Connection.TRANSACTION_READ_COMMITTED, true).createStatement();
                try {
                    backupConn.incrementQueryCount();
                    ResultSet results=stmt.executeQuery(sqlString=SB.toString());
                    try {
                        currentBatchSize=0;
                        while(results.next()) {
                            currentPaths[currentBatchSize]=results.getInt(1);
                            currentParents[currentBatchSize]=results.getInt(2);
                            currentBatchSize++;
                        }
                    } finally {
                        results.close();
                    }
                } catch(SQLException err) {
                    System.err.println("Error from query: "+sqlString);
                    throw err;
                } finally {
                    stmt.close();
                }

                if(currentBatchSize>0) {
                    // Delete the file paths
                    SB.setLength(0);
                    SB.append("delete from file_paths where pkey in (");
                    for(int c=0;c<currentBatchSize;c++) {
                        if(c>0) SB.append(',');
                        SB.append(currentPaths[c]);
                    }
                    SB.append(')');
                    backupConn.executeUpdate(SB.toString());

                    // Build a list of unique parents for the next pass
                    int uniqueParentCount=0;
                    for(int c=0;c<currentBatchSize;c++) {
                        int parent=currentParents[c];
                        boolean found=false;
                        for(int d=0;d<uniqueParentCount;d++) {
                            if(currentPaths[d]==parent) {
                                found=true;
                                break;
                            }
                        }
                        if(!found) {
                            currentPaths[uniqueParentCount++]=parent;
                        }
                    }
                    currentBatchSize=uniqueParentCount;
                }
            }
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }

    /**
     * Removes a single file path if it is not referenced by file_backups anymore.
     */
    public static void removeUnusedFilePath(
        BackupDatabaseConnection backupConn,
        int file_path
    ) throws IOException, SQLException {
        Profiler.startProfile(Profiler.UNKNOWN, FilePathHandler.class, "removeUnusedFilePath(BackupDatabaseConnection,int)", null);
        try {
            removeUnusedFilePaths(backupConn, new int[] {file_path}, 1);
        } finally {
            Profiler.endProfile(Profiler.UNKNOWN);
        }
    }
}