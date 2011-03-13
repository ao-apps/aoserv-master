/*
 * Copyright 2007-2011 by AO Industries, Inc.,
 * 7262 Bull Pen Cir, Mobile, Alabama, 36695, U.S.A.
 * All rights reserved.
 */
package com.aoindustries.aoserv.master.cluster;

import com.aoindustries.util.StringUtility;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Finds the optimal mapping of virtual machines to physical resources to balance customer needs and redundant resources.
 *
 * TODO: Provide separate control over secondary processor type, currently only secondary architecture and number of cores are considered.
 * TODO: Have separate core count and CPU weight.  Cores<=count on box, sum of primary weight <= # of cores on box, each virtualServer adds cores*weight to overall weight.
 * TODO: Make sure can actually map extents and spindle counts to drives.
 * TODO: If two virtual servers are interchangeable, don't try both combinations - implications?
 * @author  AO Industries, Inc.
 */
public final class OriginalClusterOptimizer {

    private static final int EXTENTS_SIZE = 33554432;

    private enum Rack {
        FC_9_07,
        FC_9_14,
        FC_9_17,
        KC
    }

    private enum ProcessorType {
        P4,
        P4_XEON,
        CORE,
        CORE2,
        XEON_LV
    }

    private enum ProcessorArchitecture {
        I686,
        X86_64
    }

    private enum DiskType {
        RAID1_7200,
        RAID1_10000,
        RAID5_10000,
        RAID1_15000
    }
    
    private enum SkipType {
        PRIMARY_PROCESSOR_MISMATCH,
        PRIMARY_CORES_EXCEEDED,
        PRIMARY_RAM_EXCEEDED,
        PRIMARY_PLUS_SECONDARY_RAM_EXCEEDED,
        PRIMARY_PLUS_SECONDARY_DISK_EXTENTS_EXCEEDED,
        PRIMARY_PLUS_SECONDARY_DISK_ARRAYS_EXCEEDED,
        SECONDARY_PROCESSOR_MISMATCH,
        SECONDARY_CORES_EXCEEDED,
        SECONDARY_RAM_EXCEEDED,
        SECONDARY_DISK_EXTENTS_EXCEEDED,
        SECONDARY_DISK_ARRAYS_EXCEEDED
        ;

        public long counter = 0;
    }

    private static final DiskType[] diskTypes = DiskType.values();

    private static class Disk {
        private final String device;
        private final DiskType diskType;
        private final int extents;
        
        private Disk(String device, DiskType diskType, int extents) {
            this.device = device;
            this.diskType = diskType;
            this.extents = extents;
        }
    }

    private static class Server implements Comparable<Server> {
        private final String hostname;
        private final Rack rack;
        private final int ram;
        private final ProcessorType processorType;
        private final ProcessorArchitecture processorArchitecture;
        private final int processorSpeed;
        private final int processorCores;
        private final Disk[] disks;
        
        private Server(String hostname, Rack rack, int ram, ProcessorType processorType, ProcessorArchitecture processorArchitecture, int processorSpeed, int processorCores, Disk[] disks) {
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
        @Override
        public int compareTo(Server other) {
            if(ram<other.ram) return -1;
            if(ram>other.ram) return 1;
            return processorCores-other.processorCores;
        }
    }

    private static class VirtualDisk {
        private final String device;
        private final int extents;
        private final DiskType primaryDiskType;
        private final float primaryAllocation;
        private final DiskType secondaryDiskType;
        private final float secondaryAllocation;

        private VirtualDisk(
            String device,
            int extents,
            DiskType primaryDiskType,
            float primaryAllocation,
            DiskType secondaryDiskType,
            float secondaryAllocation
        ) {
            this.device = device;
            this.extents = extents;
            this.primaryDiskType = primaryDiskType;
            this.primaryAllocation = primaryAllocation;
            this.secondaryDiskType = secondaryDiskType;
            this.secondaryAllocation = secondaryAllocation;
        }
    }

    private static class VirtualServer implements Comparable<VirtualServer> {
        private final String hostname;
        private final int minimumPrimaryRam;
        private final int minimumSecondaryRam;
        private final ProcessorType minimumProcessorType;
        private final ProcessorArchitecture requiredProcessorArchitecture;
        private final int minimumProcessorSpeed;
        private final float minimumProcessorCores;
        private final VirtualDisk[] virtualDisks;
        
        private VirtualServer(
            String hostname,
            int minimumPrimaryRam,
            int minimumSecondaryRam,
            ProcessorType minimumProcessorType,
            ProcessorArchitecture requiredProcessorArchitecture,
            int minimumProcessorSpeed,
            float minimumProcessorCores,
            VirtualDisk[] virtualDisks
        ) {
            this.hostname = hostname;
            this.minimumPrimaryRam = minimumPrimaryRam;
            this.minimumSecondaryRam = minimumSecondaryRam;
            this.minimumProcessorType = minimumProcessorType;
            this.requiredProcessorArchitecture = requiredProcessorArchitecture;
            this.minimumProcessorSpeed = minimumProcessorSpeed;
            this.minimumProcessorCores = minimumProcessorCores;
            this.virtualDisks = virtualDisks;
        }

        /**
         * Sorts from biggest to smallest.
         */
        @Override
        public int compareTo(VirtualServer other) {
            if(minimumPrimaryRam<other.minimumPrimaryRam) return 1;
            if(minimumPrimaryRam>other.minimumPrimaryRam) return -1;
            return Float.compare(other.minimumProcessorCores, minimumProcessorCores);
        }
    }

    /**
     * Need to load this directly from the servers.
     */
    private static List<Server> getServers() {
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
                    new Disk("/dev/sdd1", DiskType.RAID1_7200, 8700)  // These are the internal drives - Need separate hot-swap pair
                }
            )
        );
        Collections.sort(servers);
        return servers;
    }

    /**
     * TODO: Load this directly from the database.
     */
    private static List<VirtualServer> getVirtualServers() {
        List<VirtualServer> virtualServers = new ArrayList<VirtualServer>();
        virtualServers.add(
            new VirtualServer(
                "ao1.kc.aoindustries.com",
                512,
                512,
                null,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "centos5.aoindustries.com",
                256,
                256,
                ProcessorType.XEON_LV,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "centos5-build64.aoindustries.com",
                256,
                256,
                ProcessorType.XEON_LV,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "daissystems.com",
                1024,
                1024,
                null,
                null,
                -1,
                0.5f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 4480, DiskType.RAID1_7200, .5f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ipharos.com",
                4096,
                4096,
                ProcessorType.XEON_LV,
                null,
                -1,
                1.0f, // 4 * .25
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_15000, .5f, DiskType.RAID1_7200, .5f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "db1.fc.ipharos.com",
                2048, // Need 4096
                2048, // Need 4096
                ProcessorType.XEON_LV,
                null,
                2000,
                3.0f, // 4 * .75 - Need 4 * 1.0
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_15000, .5f, DiskType.RAID1_7200, 1.0f) // Need to be 1792, .5, .5 once ipharos.com is gone - and secondary on 15k
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.ipharos.com",
                2048, // Need 4096
                2048, // Need 4096
                ProcessorType.XEON_LV,
                null,
                2333,
                1.0f, // 4 * .25 - Need 4 * 1.0
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .5f, DiskType.RAID1_7200, .5f) // Need to be 1792, .5, .5 once ipharos.com is gone
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "master.aoindustries.com",
                1024,
                1024,
                null,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ns1.aoindustries.com",
                256,
                256,
                null,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "ns4.aoindustries.com",
                256,
                256,
                null,
                null,
                -1,
                .0625f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "w1.fc.insightsys.com",
                2048,
                2048,
                null,
                null,
                -1,
                .5f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1.0f, DiskType.RAID1_7200, 1.0f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.enduraquest.com",
                4096,
                4096,
                ProcessorType.CORE2,
                null,
                -1,
                2.0f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_10000, 1.0f, DiskType.RAID1_7200, .25f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.lnxhosting.ca",
                4096,
                4096,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.X86_64,
                3200,
                2.0f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f)
                    // new VirtualDisk("/dev/xvdb", 8064, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .03125f) // Was 
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "backup1.lnxhosting.ca",
                512,
                512,
                ProcessorType.P4_XEON,
                ProcessorArchitecture.X86_64,
                3200,
                0.5f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f),
                    new VirtualDisk("/dev/xvdb", 8064+896*4, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.newmediaworks.com",
                4096,
                4096,
                null,
                null,
                -1,
                2.0f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1.0f, DiskType.RAID1_7200, .25f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.objectevolution.com",
                1024,
                1024,
                null,
                null,
                -1,
                0.5f,
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 896*2, DiskType.RAID1_7200, .5f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.showsandshoots.com",
                512, // Need 1024
                512, // Need 1024
                null,
                null,
                -1,
                0.5f,
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.fc.softwaremiracles.com",
                512,
                512,
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .125f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.kc.aoindustries.com",
                2048,
                2048,
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 3584+896, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.kc.artizen.com",
                1024,
                1024,
                null,
                null,
                -1,
                0.5f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www1.nl.pertinence.net",
                2048,
                2048,
                null,
                null,
                -1,
                1.0f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 4480+896*2, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www2.fc.newmediaworks.com",
                4096,
                4096,
                null,
                null,
                -1,
                2.0f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1.0f, DiskType.RAID1_7200, .25f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www2.kc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688+896, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www3.kc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 0.25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www4.kc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 0.25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www5.kc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 0.25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www6.kc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 0.25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www7.fc.aoindustries.com",
                1024,
                0, // Need 1024
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 1792+896, DiskType.RAID1_7200, 0.25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www8.kc.aoindustries.com",
                2048,
                0, // Need 2048
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 2688+896, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www9.fc.aoindustries.com",
                1024,
                1024,
                null,
                null,
                -1,
                .25f,
                new VirtualDisk[] {
            // TODO: More disk +896 here
                    new VirtualDisk("/dev/xvda", 1792+896, DiskType.RAID1_7200, 0.125f, DiskType.RAID1_7200, .125f)
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www.keepandshare.com",
                8192,
                4096, // Need 8192
                ProcessorType.XEON_LV,
                null,
                2333,
                6.0f, // 8 * .75 each - Need 8 * 1.0
                new VirtualDisk[] {
                    new VirtualDisk("/dev/xvda", 7450*2, DiskType.RAID1_7200, 1.0f, DiskType.RAID1_7200, .5f) // TODO: Estimated size
                }
            )
        );
        virtualServers.add(
            new VirtualServer(
                "www.swimconnection.com",
                1024,
                1024,
                null,
                null,
                -1,  // TODO: If possible, make this 3200, had solution at -1
                1.0f,
                new VirtualDisk[] {
                    // TODO: More disk I/O here
                    new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, .25f, DiskType.RAID1_7200, .0625f)
                }
            )
        );
        Collections.sort(virtualServers);
        return virtualServers;
    }

    public static void main(String[] args) {
        List<Server> servers = getServers();
        long totalProcessorCores = 0;
        long totalRam = 0;
        long totalDisk = 0;
        int totalDiskArrays = 0;
        for(Server server : servers) {
            totalProcessorCores += server.processorCores;
            totalRam += server.ram;
            for(Disk disk : server.disks) {
                totalDisk += disk.extents;
                totalDiskArrays++;
            }
        }
        System.out.println("Servers:");
        System.out.println("    Total Processor Cores........: " + totalProcessorCores);
        System.out.println("    Total RAM....................: " + totalRam + " MB (" + StringUtility.getApproximateSize(totalRam*1048576)+")");
        System.out.println("    Total Disk Space.............: " + totalDisk + " extents (" + StringUtility.getApproximateSize(totalDisk*EXTENTS_SIZE)+")");
        System.out.println("    Total Disk Arrays............: " + totalDiskArrays);

        List<VirtualServer> virtualServers = getVirtualServers();
        float totalVirtualProcessorCores = 0;
        long totalMinimumRam = 0;
        long totalVirtualDisk = 0;
        float totalVirtualDiskArrays = 0;
        for(VirtualServer virtualServer : virtualServers) {
            totalVirtualProcessorCores += virtualServer.minimumProcessorCores;
            totalMinimumRam += virtualServer.minimumPrimaryRam;
            for(VirtualDisk virtualDisk : virtualServer.virtualDisks) {
                totalVirtualDisk += virtualDisk.extents;
                totalVirtualDiskArrays += virtualDisk.primaryAllocation + virtualDisk.secondaryAllocation;
            }
        }
        System.out.println("Virtual Servers:");
        System.out.println("    Total Minimum Processor Cores: " + totalVirtualProcessorCores);
        System.out.println("    Total Minimum RAM............: " + totalMinimumRam + " MB (" + StringUtility.getApproximateSize(totalMinimumRam*1048576)+")");
        System.out.println("    Total Virtual Disk Space.....: " + totalVirtualDisk + " extents (" + StringUtility.getApproximateSize(totalVirtualDisk*2*EXTENTS_SIZE)+")");
        System.out.println("    Total Virtual Disk Arrays....: " + totalVirtualDiskArrays);
        
        /*
         * Try all permutations of mappings from virtual server to physical servers, only continuing to the next allocation
         * checks if the total CPU cores, RAM, number of disk arrays, and disk extents is <= what the physical hardware provides.
         * Also only map the permutations when they match minimumProcessorType, requiredProcessorArchitecture, and minimumProcessorSpeed.
         * These are the quick checks that don't need to worry about the actual mappings to specific primary and secondary disk
         * arrays.
         */
        //System.out.println(servers.size());
        //System.out.println(virtualServers.size());
        System.out.println("Worst-case permutations: " + (Math.pow(servers.size(), virtualServers.size()) * Math.pow(servers.size()-1, virtualServers.size())));
        int[] selectedPrimaries = new int[virtualServers.size()];
        int[] selectedSecondaries = new int[virtualServers.size()];
        mapServers(servers, virtualServers, selectedPrimaries, selectedSecondaries, 0);
        System.out.println("Done!!!  Mapped "+mapped);
    }
    
    private static long mapped=0;
    private static long skipped=0;
    private static long lastMapDisplayedTime = -1;
    private static long callCounter = 0;

    /**
     * TODO: How can we optimize further knowing that multiple virtual servers have exactly the same configuration (and therefore don't affect the overall results when switched positions)
     */
    private static void mapServers(List<Server> servers, List<VirtualServer> virtualServers, int[] selectedPrimaries, int[] selectedSecondaries, int currentVirtualServer) {
        final int serversSize = servers.size();
        final int virtualServersSize = virtualServers.size();

        callCounter++;
        long currentTime = System.currentTimeMillis();
        long timeSince = currentTime-lastMapDisplayedTime;
        if(lastMapDisplayedTime==-1 || timeSince<0 || timeSince>=30000) {
            if(mapped!=0 || skipped!=0) {
                for(int d=0;d<currentVirtualServer;d++) {
                    if(d>0) System.out.print('/');
                    //if(selectedPrimaries[d]<10) System.out.print('0');
                    System.out.print(selectedPrimaries[d]);
                    System.out.print('.');
                    //if(selectedSecondaries[d]<10) System.out.print('0');
                    System.out.print(selectedSecondaries[d]);
                }
                System.out.print(" Mapped "+mapped+", skipped "+skipped);
                if(mapped!=0) System.out.print(", skip/map ratio: "+BigDecimal.valueOf(skipped*100/mapped, 3));
                if(timeSince>0) System.out.print(", "+(callCounter*1000/timeSince)+" calls/sec");
                System.out.println();
                /*
                for(SkipType skipType : SkipType.values()) {
                    System.out.print(skipType.name());
                    System.out.print(' ');
                    for(int c=skipType.name().length(); c<44; c++) System.out.print(' ');
                    System.out.println(skipType.counter);
                }
                 */
            }
            lastMapDisplayedTime = currentTime;
            callCounter = 0;
        }

        if(currentVirtualServer==virtualServersSize) {
            // Verify mapping
            // TODO: Make sure that each server has at least one possible mapping of virtual disk space to physical drives (both primary and secondary)
            // TODO: that provides proper disk array isolation and space allocation.
            // TODO: For each server, verify that all virtual primary and secondary (per single failure) servers match requirements.
            mapped++;
            /*
            System.out.println("Mapping found with "+skipped+" skips");
            for(int serverIndex=0;serverIndex<serversSize;serverIndex++) {
                Server server = servers.get(serverIndex);
                System.out.println(server.hostname);
                System.out.println("    Primary:");
                for(int virtualServerIndex=0;virtualServerIndex<virtualServersSize;virtualServerIndex++) {
                    if(selectedPrimaries[virtualServerIndex]==serverIndex) {
                        VirtualServer virtualServer = virtualServers.get(virtualServerIndex);
                        System.out.println("        "+virtualServer.hostname+":");
                        System.out.println("            Processor Cores: "+virtualServer.minimumProcessorCores);
                        System.out.println("            Primary RAM....: "+virtualServer.minimumPrimaryRam);
                        for(VirtualDisk virtualDisk : virtualServer.virtualDisks) {
                            System.out.println("            Device: "+virtualDisk.device);
                            System.out.println("                32MB Extents......: "+virtualDisk.extents);
                            System.out.println("                Primary Type......: "+virtualDisk.primaryDiskType);
                            System.out.println("                Primary Allocation: "+virtualDisk.primaryAllocation);
                        }
                    }
                }
                System.out.println("        Total:");
                System.out.println("    Secondary:");
                for(int failedPrimaryServerIndex=0;failedPrimaryServerIndex<serversSize;failedPrimaryServerIndex++) {
                    boolean isFirst = true;
                    for(int virtualServerIndex=0;virtualServerIndex<virtualServersSize;virtualServerIndex++) {
                        if(selectedPrimaries[virtualServerIndex]==failedPrimaryServerIndex && selectedSecondaries[virtualServerIndex]==serverIndex) {
                            Server failedPrimaryServer = servers.get(failedPrimaryServerIndex);
                            VirtualServer secondaryVirtualServer = virtualServers.get(virtualServerIndex);
                            if(isFirst) {
                                System.out.println("        From "+failedPrimaryServer.hostname+":");
                                isFirst = false;
                            }
                            System.out.println("            "+secondaryVirtualServer.hostname);
                            System.out.println("                Processor Cores: "+secondaryVirtualServer.minimumProcessorCores);
                            System.out.println("                Secondary RAM..: "+secondaryVirtualServer.minimumSecondaryRam);
                            for(VirtualDisk virtualDisk : secondaryVirtualServer.virtualDisks) {
                                System.out.println("                Device: "+virtualDisk.device);
                                System.out.println("                    32MB Extents........: "+virtualDisk.extents);
                                System.out.println("                    Secondary Type......: "+virtualDisk.secondaryDiskType);
                                System.out.println("                    Secondary Allocation: "+virtualDisk.secondaryAllocation);
                            }
                        }
                    }
                    if(!isFirst) {
                        System.out.println("            Total:");
                    }
                }
            }
            System.exit(0);
             */
        } else {
            final VirtualServer virtualServer = virtualServers.get(currentVirtualServer);
            for(int primaryServerIndex=0; primaryServerIndex<serversSize; primaryServerIndex++) {
                // Only map the virtual server to the primary server if it matches any processor type, architecture, and speed constraints.
                final Server primaryServer = servers.get(primaryServerIndex);
                if(
                    (
                        virtualServer.minimumProcessorType==null
                        || primaryServer.processorType.compareTo(virtualServer.minimumProcessorType)>=0
                    ) && (
                        virtualServer.requiredProcessorArchitecture==null
                        || primaryServer.processorArchitecture==virtualServer.requiredProcessorArchitecture
                    ) && (
                        virtualServer.minimumProcessorSpeed==-1
                        || primaryServer.processorSpeed>=virtualServer.minimumProcessorSpeed
                    )
                ) {
                    selectedPrimaries[currentVirtualServer]=primaryServerIndex;
                    // Stop processing if primaryServer past capacity on either processor cores or RAM
                    float totalPrimaryServerVirtualProcessorCores = 0;
                    long totalPrimaryServerMinimumRam = 0;
                    for(int d=0;d<=currentVirtualServer;d++) {
                        if(selectedPrimaries[d]==primaryServerIndex) {
                            VirtualServer mappedVirtualServer = virtualServers.get(d);
                            totalPrimaryServerVirtualProcessorCores += mappedVirtualServer.minimumProcessorCores;
                            totalPrimaryServerMinimumRam += mappedVirtualServer.minimumPrimaryRam;
                        }
                    }
                    if(primaryServer.processorCores>=totalPrimaryServerVirtualProcessorCores) {
                        if(primaryServer.ram>=totalPrimaryServerMinimumRam) {
                            // Make sure that the combined primary mappings plus secondary CPU cores and RAM do not exceed the total of this machine
                            // for any one primary failure.  The loop represents the failure of each server, one at a time.
                            boolean needsSecondarySkip = false;
                            for(int failedPrimaryServerIndex=0;failedPrimaryServerIndex<serversSize;failedPrimaryServerIndex++) {
                                if(failedPrimaryServerIndex!=primaryServerIndex) {
                                    //float totalSecondaryVirtualProcessorCores = totalPrimaryServerVirtualProcessorCores;
                                    long totalSecondaryMinimumRam = totalPrimaryServerMinimumRam;
                                    for(int f=0;f<currentVirtualServer;f++) {
                                        if(selectedPrimaries[f]==failedPrimaryServerIndex && selectedSecondaries[f]==primaryServerIndex) {
                                            VirtualServer mappedVirtualServer = virtualServers.get(f);
                                            //totalSecondaryVirtualProcessorCores += mappedVirtualServer.minimumProcessorCores;
                                            totalSecondaryMinimumRam += mappedVirtualServer.minimumSecondaryRam;
                                        }
                                    }
                                    if(
                                        /*primaryServer.processorCores<totalSecondaryVirtualProcessorCores
                                        &&*/ primaryServer.ram<totalSecondaryMinimumRam
                                    ) {
                                        needsSecondarySkip = true;
                                        break;
                                    }
                                }
                            }
                            if(!needsSecondarySkip) {
                                // For each disk type, skip if exceeded on either total extents or arrays (allocation)
                                // TODO: Add in secondary volumes here, too
                                boolean needsSkip = false;
                                for(int e=0;e<diskTypes.length;e++) {
                                    DiskType diskType = diskTypes[e];
                                    // Calculate total required
                                    long totalVirtualDisk = 0;
                                    float totalVirtualDiskArrays = 0;
                                    for(int d=0;d<=currentVirtualServer;d++) {
                                        if(selectedPrimaries[d]==primaryServerIndex) {
                                            VirtualServer mappedVirtualServer = virtualServers.get(d);
                                            for(VirtualDisk virtualDisk : mappedVirtualServer.virtualDisks) {
                                                if(virtualDisk.primaryDiskType==diskType) {
                                                    totalVirtualDisk += virtualDisk.extents;
                                                    totalVirtualDiskArrays += virtualDisk.primaryAllocation;
                                                }
                                            }
                                        }
                                    }
                                    for(int d=0;d<currentVirtualServer;d++) {
                                        if(selectedSecondaries[d]==primaryServerIndex) {
                                            VirtualServer mappedVirtualServer = virtualServers.get(d);
                                            for(VirtualDisk virtualDisk : mappedVirtualServer.virtualDisks) {
                                                if(virtualDisk.secondaryDiskType==diskType) {
                                                    totalVirtualDisk += virtualDisk.extents;
                                                    totalVirtualDiskArrays += virtualDisk.secondaryAllocation;
                                                }
                                            }
                                        }
                                    }
                                    // Calculate total provided by the primaryServer
                                    long totalDiskExtents = 0;
                                    int totalDiskArrays = 0;
                                    for(Disk disk : primaryServer.disks) {
                                        if(disk.diskType==diskType) {
                                            totalDiskExtents+=disk.extents;
                                            totalDiskArrays++;
                                        }
                                    }
                                    if(totalDiskExtents<totalVirtualDisk) {
                                        SkipType.PRIMARY_PLUS_SECONDARY_DISK_EXTENTS_EXCEEDED.counter++;
                                        needsSkip = true;
                                        break;
                                    } else if(totalDiskArrays<totalVirtualDiskArrays) {
                                        SkipType.PRIMARY_PLUS_SECONDARY_DISK_ARRAYS_EXCEEDED.counter++;
                                        needsSkip = true;
                                        break;
                                    }
                                }
                                if(!needsSkip) {
                                    // Now try each of the possible secondary mappings (to all servers except the primary)
                                    for(int secondaryServerIndex=0; secondaryServerIndex<serversSize; secondaryServerIndex++) {
                                        if(secondaryServerIndex!=primaryServerIndex) {
                                            final Server secondaryServer = servers.get(secondaryServerIndex);
                                            selectedSecondaries[currentVirtualServer]=secondaryServerIndex;

                                            // Make sure the secondary architecture matches any requirements
                                            if(virtualServer.requiredProcessorArchitecture==null || secondaryServer.processorArchitecture==virtualServer.requiredProcessorArchitecture) {
                                                // Make sure secondary has at least total number of cores matching secondary cores.
                                                if(secondaryServer.processorCores>=virtualServer.minimumProcessorCores) {
                                                    // Make sure that the combined primary mapping plus secondary CPU cores and RAM do not exceed the total of this machine
                                                    // for any one primary failure.  The loop represents the failure of each server, one at a time.
                                                    //float totalPrimaryVirtualProcessorCores = 0;
                                                    long totalPrimaryMinimumRam = 0;
                                                    for(int g=0;g<=currentVirtualServer;g++) {
                                                        if(selectedPrimaries[g]==secondaryServerIndex) {
                                                            VirtualServer mappedVirtualServer = virtualServers.get(g);
                                                            //totalPrimaryVirtualProcessorCores += mappedVirtualServer.minimumProcessorCores;
                                                            totalPrimaryMinimumRam += mappedVirtualServer.minimumPrimaryRam;
                                                        }
                                                    }
                                                    needsSecondarySkip = false;
                                                    for(int failedPrimaryIndex=0;failedPrimaryIndex<serversSize;failedPrimaryIndex++) {
                                                        if(failedPrimaryIndex!=secondaryServerIndex) {
                                                            //float totalSecondaryVirtualProcessorCores = totalPrimaryVirtualProcessorCores;
                                                            long totalSecondaryMinimumRam = totalPrimaryMinimumRam;
                                                            for(int h=0;h<=currentVirtualServer;h++) {
                                                                if(selectedPrimaries[h]==failedPrimaryIndex && selectedSecondaries[h]==secondaryServerIndex) {
                                                                    VirtualServer mappedVirtualServer = virtualServers.get(h);
                                                                    //totalSecondaryVirtualProcessorCores += mappedVirtualServer.minimumProcessorCores;
                                                                    totalSecondaryMinimumRam += mappedVirtualServer.minimumSecondaryRam;
                                                                }
                                                            }
                                                            if(
                                                                /*secondaryServer.processorCores<totalSecondaryVirtualProcessorCores
                                                                &&*/ secondaryServer.ram<totalSecondaryMinimumRam
                                                            ) {
                                                                needsSecondarySkip = true;
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    if(!needsSecondarySkip) {
                                                        // For each disk type, skip if exceeded on either total extents or arrays (allocation)
                                                        needsSkip = false;
                                                        for(int e=0;e<diskTypes.length;e++) {
                                                            DiskType diskType = diskTypes[e];
                                                            // Calculate total required for primaries
                                                            long totalVirtualDisk = 0;
                                                            float totalVirtualDiskArrays = 0;
                                                            for(int d=0;d<=currentVirtualServer;d++) {
                                                                if(selectedPrimaries[d]==secondaryServerIndex) {
                                                                    VirtualServer mappedVirtualServer = virtualServers.get(d);
                                                                    for(VirtualDisk virtualDisk : mappedVirtualServer.virtualDisks) {
                                                                        if(virtualDisk.primaryDiskType==diskType) {
                                                                            totalVirtualDisk += virtualDisk.extents;
                                                                            totalVirtualDiskArrays += virtualDisk.primaryAllocation;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            // Also add total required for secondaries
                                                            for(int d=0;d<=currentVirtualServer;d++) {
                                                                if(selectedSecondaries[d]==secondaryServerIndex) {
                                                                    VirtualServer mappedVirtualServer = virtualServers.get(d);
                                                                    for(VirtualDisk virtualDisk : mappedVirtualServer.virtualDisks) {
                                                                        if(virtualDisk.secondaryDiskType==diskType) {
                                                                            totalVirtualDisk += virtualDisk.extents;
                                                                            totalVirtualDiskArrays += virtualDisk.secondaryAllocation;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            // Calculate total provided by the secondaryServer
                                                            long totalDiskExtents = 0;
                                                            int totalDiskArrays = 0;
                                                            for(Disk disk : secondaryServer.disks) {
                                                                if(disk.diskType==diskType) {
                                                                    totalDiskExtents+=disk.extents;
                                                                    totalDiskArrays++;
                                                                }
                                                            }
                                                            if(totalDiskExtents<totalVirtualDisk) {
                                                                needsSkip = true;
                                                                SkipType.SECONDARY_DISK_EXTENTS_EXCEEDED.counter++;
                                                                break;
                                                            } else if(totalDiskArrays<totalVirtualDiskArrays) {
                                                                needsSkip = true;
                                                                SkipType.SECONDARY_DISK_ARRAYS_EXCEEDED.counter++;
                                                                break;
                                                            }
                                                        }
                                                        if(!needsSkip) {
                                                            mapServers(servers, virtualServers, selectedPrimaries, selectedSecondaries, currentVirtualServer+1);
                                                        } else {
                                                            skipped++;
                                                        }
                                                    } else {
                                                        skipped++;
                                                        SkipType.SECONDARY_RAM_EXCEEDED.counter++;
                                                    }
                                                } else {
                                                    skipped++;
                                                    SkipType.SECONDARY_CORES_EXCEEDED.counter++;
                                                }
                                            } else {
                                                skipped++;
                                                SkipType.SECONDARY_PROCESSOR_MISMATCH.counter++;
                                            }
                                        }
                                    }
                                } else {
                                    skipped++;
                                }
                            } else {
                                skipped++;
                                SkipType.PRIMARY_PLUS_SECONDARY_RAM_EXCEEDED.counter++;
                            }
                        } else {
                            skipped++;
                            SkipType.PRIMARY_RAM_EXCEEDED.counter++;
                        }
                    } else {
                        skipped++;
                        SkipType.PRIMARY_CORES_EXCEEDED.counter++;
                    }
                } else {
                    skipped++;
                    SkipType.PRIMARY_PROCESSOR_MISMATCH.counter++;
                }
            }
        }
    }
}
