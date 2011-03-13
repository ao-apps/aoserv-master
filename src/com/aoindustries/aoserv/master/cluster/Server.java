package com.aoindustries.aoserv.master.cluster;

/*
 * Copyright 2007-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
public final class Server implements Comparable<Server> {

    /**
     * Need to load this directly from the servers.
     */
    static Server[] getServers() {
        List<Server> servers = new ArrayList<Server>();
        servers.add(
            new Server(
                "gw1.fc.aoindustries.com",
                Rack.FC_9_07,
                2048,
                ProcessorType.P4,
                ProcessorArchitecture.I686,
                2800,
                2,
                new Disk[] {
                    new Disk("/dev/md0", DiskType.RAID1_7200, 2044),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 2044)
                }
            )
        );
        servers.add(
            new Server(
                "xen907-1.fc.aoindustries.com",
                Rack.FC_9_07,
                16384,
                ProcessorType.XEON_LV,
                ProcessorArchitecture.X86_64,
                2000,
                8,
                new Disk[] {
                    new Disk("/dev/sde1", DiskType.RAID1_7200, 7450),
                    new Disk("/dev/sdf1", DiskType.RAID1_7200, 7450),
                    new Disk("/dev/sdg1", DiskType.RAID1_7200, 7450),
                    new Disk("/dev/sdh1", DiskType.RAID1_7200, 7450),
                    new Disk("/dev/sdi1", DiskType.RAID1_7200, 7450*2), // TODO: These are not purchased yet - estimated size
                    new Disk("/dev/md3", DiskType.RAID1_15000, 4375)
                }
            )
        );
        servers.add(
            new Server(
                "xen907-2.fc.aoindustries.com",
                Rack.FC_9_07,
                2048,
                ProcessorType.P4,
                ProcessorArchitecture.I686,
                2600,
                2,
                new Disk[] {
                    new Disk("/dev/md1", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md2", DiskType.RAID1_7200, 7139),
                    new Disk("/dev/md5", DiskType.RAID1_7200, 4769)
                }
            )
        );
        servers.add(
            new Server(
                "xen907-5.fc.aoindustries.com",
                Rack.FC_9_07,
                4096,
                ProcessorType.P4,
                ProcessorArchitecture.X86_64,
                3400,
                2,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 2102),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 7112)
                }
            )
        );
        servers.add(
            new Server(
                "gw2.fc.aoindustries.com",
                Rack.FC_9_14,
                2048,
                ProcessorType.P4,
                ProcessorArchitecture.I686,
                2800,
                2,
                new Disk[] {
                    new Disk("/dev/md1", DiskType.RAID1_7200, 3236),
                    new Disk("/dev/md2", DiskType.RAID1_7200, 1449)
                }
            )
        );
        servers.add(
            new Server(
                "xen914-1.fc.aoindustries.com",
                Rack.FC_9_14,
                2048,
                ProcessorType.P4,
                ProcessorArchitecture.I686,
                2800,
                2,
                new Disk[] {
                    new Disk("/dev/md1", DiskType.RAID1_7200, 2044),
                    new Disk("/dev/md2", DiskType.RAID1_7200, 3340)
                }
            )
        );
        servers.add(
            new Server(
                "xen914-2.fc.aoindustries.com",
                Rack.FC_9_14,
                4096,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.I686,
                2400,
                4,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md5", DiskType.RAID5_10000, 1093)
                }
            )
        );
        servers.add(
            new Server(
                "xen914-5.fc.lnxhosting.ca",
                Rack.FC_9_14,
                16384,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.X86_64,
                3200,
                8,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 7450*4), // TODO: These are not purchased yet - estimated size
                    new Disk("/dev/md4", DiskType.RAID1_7200, 9198),
                    new Disk("/dev/md5", DiskType.RAID1_7200, 9198)
                }
            )
        );
        /* powered-down after installation of xen917-5.fc.aoindustries.com
        servers.add(
            new Server(
                "xen917-1.fc.aoindustries.com",
                Rack.FC_9_17,
                2048,
                ProcessorType.P4,
                ProcessorArchitecture.I686,
                3200,
                2,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md5", DiskType.RAID1_7200, 3236)
                }
            )
        );
         */
        servers.add(
            new Server(
                "xen917-2.fc.aoindustries.com",
                Rack.FC_9_17,
                2048,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.I686,
                2667,
                4,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 2044),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 2044)
                }
            )
        );
        servers.add(
            new Server(
                "xen917-3.fc.aoindustries.com",
                Rack.FC_9_17,
                6144,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.X86_64,
                2800,
                4,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_7200, 7112),
                    new Disk("/dev/md4", DiskType.RAID1_7200, 7139)
                }
            )
        );
        servers.add(
            new Server(
                "xen917-4.fc.aoindustries.com",
                Rack.FC_9_17,
                4096,
                ProcessorType.CORE2,
                ProcessorArchitecture.X86_64,
                2130,
                2,
                new Disk[] {
                    new Disk("/dev/md3", DiskType.RAID1_10000, 1851)
                }
            )
        );
        servers.add(
            new Server(
                "xen917-5.fc.aoindustries.com",
                Rack.FC_9_17,
                16384,
                ProcessorType.XEON_LV,
                ProcessorArchitecture.X86_64,
                2333,
                8,
                new Disk[] {
                    new Disk("/dev/sdc1", DiskType.RAID1_7200, 7450*2), // TODO: These are not purchased yet.  Estimated size for the 2x500 GB for CARR
                    new Disk("/dev/sdb1", DiskType.RAID1_7200, 9536),
                    new Disk("/dev/sde1", DiskType.RAID1_7200, 7450*2), // TODO: Not purchased yet, estimated size
                    new Disk("/dev/md3", DiskType.RAID1_15000, 4375)
                }
            )
        );
        Collections.sort(servers);
        // Collections.shuffle(servers);
        Server[] array = servers.toArray(new Server[servers.size()]);
        for(Server server : array) server.allocatedSecondaryRAMs = new int[array.length];
        return array;
    }

    final String hostname;
    final Rack rack;
    final int ram;
    final ProcessorType processorType;
    final ProcessorArchitecture processorArchitecture;
    final int processorSpeed;
    final int processorCores;
    final Disk[] disks;

    /**
     * The primary RAM allocated during the recursive processing.
     */
    int allocatedPrimaryRAM;

    /**
     * The allocated secondary RAM on a per-primary-server basis.
     */
    int[] allocatedSecondaryRAMs = null;

    /**
     * The maximum secondary RAM allocated on any of the primary servers.
     */
    int maximumAllocatedSecondaryRAM = 0;

    /**
     * The allocated processor weight during the recursive processing.
     */
    int allocatedProcessorWeight = 0;

    Server(String hostname, Rack rack, int ram, ProcessorType processorType, ProcessorArchitecture processorArchitecture, int processorSpeed, int processorCores, Disk[] disks) {
        this.hostname = hostname;
        this.rack = rack;
        this.ram = ram;
        this.processorType = processorType;
        this.processorArchitecture = processorArchitecture;
        this.processorSpeed = processorSpeed;
        this.processorCores = processorCores;
        this.disks = disks;
    }
        
    /**
     * Sorts from smallest to biggest.  The combination of sorting servers from smallest to biggest and virtual servers
     * from biggest to smallest causes the tightest fit of big virtual servers into the smallest possible server.  This
     * results in the smallest skip/map ratio (and hopefully quicker finding of optimal layouts).
     */
    public int compareTo(Server other) {
        /*
        return -realCompareTo(other);
    }

    public int realCompareTo(Server other) {
         */
        // By RAM
        if(ram<other.ram) return -1;
        if(ram>other.ram) return 1;
        // By Processor Cores
        int diff = processorCores-other.processorCores;
        if(diff!=0) return diff;
        /*
        // By Disk Extents
        long totalExtents = 0;
        for(Disk disk : disks) totalExtents += disk.extents;
        long otherTotalExtents = 0;
        for(Disk disk : other.disks) otherTotalExtents += disk.extents;
        if(totalExtents<otherTotalExtents) return -1;
        if(totalExtents>otherTotalExtents) return 1;
         */
        // By Biggest Disk
        int mostExtents = 0;
        for(Disk disk : disks) if(disk.extents>mostExtents) mostExtents = disk.extents;
        long otherMostExtents = 0;
        for(Disk disk : other.disks) if(disk.extents>otherMostExtents) otherMostExtents = disk.extents;
        if(mostExtents<otherMostExtents) return 1;
        if(mostExtents>otherMostExtents) return -1;

        return 0;
    }
}
