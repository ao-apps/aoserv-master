package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007 by AO Industries, Inc.,
 * 816 Azalea Rd, Mobile, Alabama, 36693, U.S.A.
 * All rights reserved.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
public final class VirtualServer implements Comparable<VirtualServer> {

    /**
     * TODO: Load this directly from the database.
     */
    static VirtualServer[] getVirtualServers() {
        List<VirtualServer> virtualServers = new ArrayList<VirtualServer>();
        virtualServers.add(
            new VirtualServer(
                "ao1.kc.aoindustries.com",
                null,
                null,
                512,
                512,
                null,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "centos5.aoindustries.com",
                null,
                null,
                256,
                256,
                ProcessorType.XEON_LV,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "centos5-build64.aoindustries.com",
                null,
                null,
                256,
                256,
                ProcessorType.XEON_LV,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "daissystems.com",
                null,
                null,
                1024,
                512, // Desire 1024
                null,
                null,
                -1,
                2,
                1, // Desire 250,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 4480, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ipharos.com",
                "xen907-1.fc.aoindustries.com",
                null,
                4096,
                4096,
                null,
                null,
                -1,
                4,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_15000, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "db1.fc.ipharos.com",
                "xen907-1.fc.aoindustries.com",
                null,
                2048, // Need 4096
                2048, // Need 4096
                null,
                null,
                -1,
                4,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_15000, 1, DiskType.RAID1_7200, 1) // Need to be 1792, .5, .5 once ipharos.com is gone - and secondary on 15k
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.ipharos.com",
                "xen917-5.fc.aoindustries.com",
                null,
                2048, // Need 4096
                2048, // Need 4096
                null,
                null,
                -1,
                4,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need to be 1792, .5, .5 once ipharos.com is gone
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "master.aoindustries.com",
                null,
                null,
                1024,
                1024,
                null,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ns1.aoindustries.com",
                null,
                null,
                256,
                256,
                null,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ns4.aoindustries.com",
                null,
                null,
                256,
                256,
                null,
                null,
                -1,
                1,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "w1.fc.insightsys.com",
                null,
                null,
                2048,
                2048,
                null,
                null,
                -1,
                1,
                1, // Desire 500,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.enduraquest.com",
                "xen917-4.fc.aoindustries.com",
                null,
                4096,
                2048, // Need 4096
                null,
                null,
                -1,
                2,
                1, // Need 1000,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_10000, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.lnxhosting.ca",
                "xen914-5.fc.lnxhosting.ca",
                null,
                4096,
                2048, // Desire 4096
                null,
                ProcessorArchitecture.X86_64,
                -1,
                4,
                1, // Desire 500,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                    // new VirtualDisk("/dev/xvdb", 8064, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f) // Was 
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "backup1.lnxhosting.ca",
                "xen914-5.fc.lnxhosting.ca",
                null,
                512,
                512,
                null,
                null,
                -1,
                2,
                1, // Desire 250,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1),
                    new VirtualDisk("/dev/xvdb", 8064, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 125) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.newmediaworks.com",
                null,
                null,
                4096,
                4096,
                null,
                null,
                -1,
                2,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.objectevolution.com",
                null,
                null,
                1024,
                1024,
                null,
                null,
                -1,
                1,
                1, // Need 1000
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 896*2, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.showsandshoots.com",
                null,
                null,
                512, // Desire 1024
                512, // Desire 1024
                null,
                null,
                -1,
                1,
                1, // Desire 1000
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.softwaremiracles.com",
                null,
                null,
                512,
                512,
                null,
                null,
                -1,
                1,
                1, // Desire 250,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.kc.aoindustries.com",
                null,
                null,
                2048,
                2048,
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 3584, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.kc.artizen.com",
                null,
                null,
                1024,
                1024,
                null,
                null,
                -1,
                1,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.nl.pertinence.net",
                null,
                null,
                2048,
                1024, // Desire 2048
                null,
                null,
                -1,
                2,
                1, // Desire 250,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 4480, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www2.fc.newmediaworks.com",
                "xen907-5.fc.aoindustries.com",
                null,
                4096,
                4096,
                null,
                null,
                -1,
                2,
                1, // Need 1000
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www2.kc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www3.kc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www4.kc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www5.kc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www6.kc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www7.fc.aoindustries.com",
                null,
                null,
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www8.kc.aoindustries.com",
                null,
                null,
                2048,
                0, // Need 2048
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www9.fc.aoindustries.com",
                null,
                null,
                1024,
                1024, // Need 1024
                null,
                null,
                -1,
                2,
                1,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // Need more space
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www.keepandshare.com",
                "xen917-5.fc.aoindustries.com",
                null,
                8192,
                4096, // Desire 8192
                null,
                null,
                -1,
                8,
                1, // Need 1000 here
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 7450*2, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1) // TODO: Estimated size
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www.swimconnection.com",
                null,
                null,
                1024,
                1024,
                null,
                null,
                -1,  // TODO: If possible, make this 3200, had solution at -1
                2,
                1, // Need 1000
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 1, DiskType.RAID1_7200, 1)
                }
            )
        );
        Collections.sort(virtualServers);
        return virtualServers.toArray(new VirtualServer[virtualServers.size()]);
    }

    final String hostname;
    final String primaryServerHostname;
    final String secondaryServerHostname;
    final int primaryRam;
    final int secondaryRam;
    final ProcessorType minimumProcessorType;
    final ProcessorArchitecture requiredProcessorArchitecture;
    final int minimumProcessorSpeed;
    final int processorCores;
    final int processorWeight;
    final VirtualDisk[] virtualDisks;

    int selectedPrimaryServerIndex = -1;
    int selectedSecondaryServerIndex = -1;

    VirtualServer(
        String hostname,
        String primaryServerHostname,
        String secondaryServerHostname,
        int primaryRam,
        int secondaryRam,
        ProcessorType minimumProcessorType,
        ProcessorArchitecture requiredProcessorArchitecture,
        int minimumProcessorSpeed,
        int processorCores,
        int processorWeight, // On a scale of 1000
        VirtualDisk[] virtualDisks
    ) {
        this.hostname = hostname;
        this.primaryServerHostname = primaryServerHostname;
        this.secondaryServerHostname = secondaryServerHostname;
        this.primaryRam = primaryRam;
        this.secondaryRam = secondaryRam;
        this.minimumProcessorType = minimumProcessorType;
        this.requiredProcessorArchitecture = requiredProcessorArchitecture;
        this.minimumProcessorSpeed = minimumProcessorSpeed;
        this.processorCores = processorCores;
        this.processorWeight = processorWeight;
        this.virtualDisks = virtualDisks;
    }

    /**
     * Sorts from biggest to smallest.
     */
    public int compareTo(VirtualServer other) {
        /*
        return -realCompareTo(other);
    }

    public int realCompareTo(VirtualServer other) {
         */
        if(primaryRam<other.primaryRam) return 1;
        if(primaryRam>other.primaryRam) return -1;
        return (other.processorCores*other.processorWeight) - (processorCores*processorWeight);
    }
}
