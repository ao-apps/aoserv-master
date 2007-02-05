package com.aoindustries.aoserv.master;

/*
 * Copyright 2003-2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */

/**
 * Temporary storage used inside <code>ReportGenerator</code>.
 *
 * @see  ReportGenerator
 *
 * @author  AO Industries, Inc.
 */
public class TempBackupReport {

    public int server;
    public int packageNum;
    public int fileCount;
    public long uncompressedSize;
    public long compressedSize;
    public long diskSize;
}