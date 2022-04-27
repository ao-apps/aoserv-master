/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2009, 2019, 2020, 2021, 2022  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoindustries.aoserv.master.cluster;

import com.aoapps.lang.Strings;
import com.aoapps.sql.SQLUtility;

/**
 * Finds the optimal mapping of virtual machines to physical resources to balance customer needs and redundant resources.
 *
 * TODO: If two virtual servers are interchangeable, don't try both combinations - how? - implications?
 * TODO: If two servers are interchangeable, don't try both combinations - how? - implications?
 * TODO: Allow to specify that two virtual servers may not use the same primary xen machine (like ns1 and ns4 in California)
 * TODO: Avoid the reprocessing of similar sub-trees to try to convert the NP-hardness to exponential complexity (with lots of RAM used)
 * TODO: Also map cores and weights onto CPUs, with the following patterns:
 *       1 core into any of the CPUs
 *       2 cores into pairs of CPUs (0-1, 2-3, 3-4, 5-6, 7-8, 9-10, ...)
 *       4 cores into sets of CPUs (0-3, 4-7, 8-11, 12-15, ...)
 *       8 cores into sets of CPUs (0-7, 8-15, ...)
 *       ...
 *       Require that cores be a power of 2, so easier to map to CPUs consistently.
 * TODO: Change weight to be 1024, so powers of two add up well (like 512+256+64+64+64+32+16+16 equals a full CPU)
 * TODO: If all else fails, integrate this with the aoserv daemon code and run massively parallel processing of subtrees.  (More brute force)
 *
 * @author  AO Industries, Inc.
 */
public final class ClusterOptimizer {

  private static final boolean FIND_ALL_PERMUTATIONS = true;

  private static final boolean TERMINATE_ON_FIRST_MATCH = true;

  private static final boolean TRACE = false;

  private static final long EXTENTS_SIZE = 33554432;

  private enum SkipType {
    PRIMARY_MANUAL_SERVER_MISMATCH,
    PRIMARY_PROCESSOR_MISMATCH,
    PRIMARY_CORES_EXCEEDED,
    PRIMARY_RAM_EXCEEDED,
    PRIMARY_DISK_TYPE_MISMATCH,
    PRIMARY_DISK_EXTENTS_EXCEEDED,
    PRIMARY_DISK_WEIGHT_EXCEEDED,
    PRIMARY_DISK_MATCHES_PREVIOUS,
    SECONDARY_MANUAL_SERVER_MISMATCH,
    SECONDARY_PROCESSOR_MISMATCH,
    SECONDARY_CORES_EXCEEDED,
    SECONDARY_RAM_EXCEEDED,
    SECONDARY_DISK_TYPE_MISMATCH,
    SECONDARY_DISK_EXTENTS_EXCEEDED,
    SECONDARY_DISK_WEIGHT_EXCEEDED,
    SECONDARY_DISK_MATCHES_PREVIOUS
    ;

    public long counter = 0;
  }

  public static void main(String[] args) {
    Host[] servers = Host.getServers();
    VirtualServer[] virtualServers = VirtualServer.getVirtualServers();
    printTotals(virtualServers, servers);
    mapVirtualServersToPrimaryServers(virtualServers, servers, 0);
    displaySkipTypes();
    System.out.println("Done!!!  Mapped " + mapped);
  }

  private static void printTotals(VirtualServer[] virtualServers, Host[] servers) {
    long totalProcessorCores = 0;
    long totalRam = 0;
    long totalDisk = 0;
    int totalDiskArrays = 0;
    for (Host server : servers) {
      totalProcessorCores += server.processorCores;
      totalRam += server.ram;
      for (Disk disk : server.disks) {
        totalDisk += disk.extents;
        totalDiskArrays++;
      }
    }
    System.out.println("Servers:");
    System.out.println("    Total Processor Cores: " + totalProcessorCores);
    System.out.println("    Total RAM............: " + totalRam + " MB (" + Strings.getApproximateSize(totalRam * 1048576) + ')');
    System.out.println("    Total Disk Space.....: " + totalDisk + " extents (" + Strings.getApproximateSize(totalDisk * EXTENTS_SIZE) + ')');
    System.out.println("    Total Disk Arrays....: " + totalDiskArrays);

    long totalVirtualProcessorAllocation = 0;
    long totalMinimumRam = 0;
    long totalVirtualDisk = 0;
    long totalVirtualDiskWeight = 0;
    for (VirtualServer virtualServer : virtualServers) {
      totalVirtualProcessorAllocation += virtualServer.processorCores * virtualServer.processorWeight;
      totalMinimumRam += virtualServer.primaryRam;
      for (VirtualDisk virtualDisk : virtualServer.virtualDisks) {
        totalVirtualDisk += virtualDisk.extents * 2;
        totalVirtualDiskWeight += virtualDisk.primaryWeight + virtualDisk.secondaryWeight;
      }
    }
    System.out.println("Virtual Servers:");
    System.out.println("    Total Processor Cores: " + SQLUtility.formatDecimal3(totalVirtualProcessorAllocation));
    System.out.println("    Total Primary RAM....: " + totalMinimumRam + " MB (" + Strings.getApproximateSize(totalMinimumRam * 1048576) + ')');
    System.out.println("    Total Disk Space.....: " + totalVirtualDisk + " extents (" + Strings.getApproximateSize(totalVirtualDisk * EXTENTS_SIZE) + ')');
    System.out.println("    Total Disk Arrays....: " + SQLUtility.formatDecimal3(totalVirtualDiskWeight));
  }

  private static long lastMapDisplayedTime = -1;
  private static long callCounter;

  private static void displayProgress(VirtualServer[] virtualServers, Host[] servers, int currentVirtualServer) {
    callCounter++;
    long currentTime = System.currentTimeMillis();
    long timeSince = currentTime - lastMapDisplayedTime;
    if (lastMapDisplayedTime == -1 || timeSince < 0 || timeSince >= 30000) {
      if (mapped != 0 || skipped != 0) {
        for (int d = 0; d < currentVirtualServer; d++) {
          if (d > 0) {
            System.out.print('/');
          }
          //if (selectedPrimaries[d]<10) {
          //  System.out.print('0');
          //}
          System.out.print(virtualServers[d].selectedPrimaryServerIndex);
          System.out.print('.');
          //if (selectedSecondaries[d]<10) {
          //  System.out.print('0');
          //}
          System.out.print(virtualServers[d].selectedSecondaryServerIndex);
        }
        System.out.print(" Mapped " + mapped + ", skipped " + skipped);
        if (mapped != 0) {
          System.out.print(", skip/map ratio: " + SQLUtility.formatDecimal2(skipped * 100 / mapped));
        }
        if (timeSince > 0) {
          System.out.print(", " + (callCounter / timeSince) + " calls/ms");
        }
        System.out.println();
        displaySkipTypes();
      }
      lastMapDisplayedTime = currentTime;
      callCounter = 0;
    }
  }

  private static void displaySkipTypes() {
    for (SkipType skipType : SkipType.values()) {
      System.out.print(skipType.name());
      System.out.print(' ');
      for (int c = skipType.name().length(); c < 44; c++) {
        System.out.print(' ');
      }
      System.out.println(skipType.counter);
    }
  }

  private static void displayMapping(VirtualServer[] virtualServers, Host[] hosts) {
    final int hostsSize = hosts.length;
    final int virtualServersSize = virtualServers.length;

    System.out.println("Mapping found with " + skipped + " skips");
    for (int hostIndex = 0; hostIndex < hostsSize; hostIndex++) {
      Host server = hosts[hostIndex];
      System.out.println(server.hostname);
      System.out.println("    Primary:");
      for (int virtualServerIndex = 0; virtualServerIndex < virtualServersSize; virtualServerIndex++) {
        if (virtualServers[virtualServerIndex].selectedPrimaryServerIndex == hostIndex) {
          VirtualServer virtualServer = virtualServers[virtualServerIndex];
          System.out.println("        " + virtualServer.hostname + ":");
          System.out.println("            Processor Cores.: " + virtualServer.processorCores);
          System.out.println("            Processor Weight: " + SQLUtility.formatDecimal3(virtualServer.processorWeight));
          System.out.println("            Primary RAM.....: " + virtualServer.primaryRam);
          for (VirtualDisk virtualDisk : virtualServer.virtualDisks) {
            System.out.println("            Device: " + virtualDisk.device);
            System.out.println("                32MB Extents..: " + virtualDisk.extents + " (" + Strings.getApproximateSize(virtualDisk.extents * EXTENTS_SIZE) + ')');
            System.out.println("                Primary Type..: " + virtualDisk.primaryDiskType);
            System.out.println("                Primary Weight: " + SQLUtility.formatDecimal3(virtualDisk.primaryWeight));
            System.out.println("                Primary Device: " + virtualDisk.selectedPrimaryDisk.device);
          }
        }
      }
      System.out.println("        Total:");
      System.out.println("    Secondary:");
      for (int failedPrimaryServerIndex = 0; failedPrimaryServerIndex < hostsSize; failedPrimaryServerIndex++) {
        boolean isFirst = true;
        for (int virtualServerIndex = 0; virtualServerIndex < virtualServersSize; virtualServerIndex++) {
          if (virtualServers[virtualServerIndex].selectedPrimaryServerIndex == failedPrimaryServerIndex && virtualServers[virtualServerIndex].selectedSecondaryServerIndex == hostIndex) {
            Host failedPrimaryServer = hosts[failedPrimaryServerIndex];
            VirtualServer secondaryVirtualServer = virtualServers[virtualServerIndex];
            if (isFirst) {
              System.out.println("        From " + failedPrimaryServer.hostname + ":");
              isFirst = false;
            }
            System.out.println("            " + secondaryVirtualServer.hostname);
            System.out.println("                Processor Cores.: " + secondaryVirtualServer.processorCores);
            System.out.println("                Processor Weight: " + SQLUtility.formatDecimal3(secondaryVirtualServer.processorWeight));
            System.out.println("                Secondary RAM...: " + secondaryVirtualServer.secondaryRam);
            for (VirtualDisk virtualDisk : secondaryVirtualServer.virtualDisks) {
              System.out.println("                Device: " + virtualDisk.device);
              System.out.println("                    32MB Extents....: " + virtualDisk.extents + " (" + Strings.getApproximateSize(virtualDisk.extents * EXTENTS_SIZE) + ')');
              System.out.println("                    Secondary Type..: " + virtualDisk.secondaryDiskType);
              System.out.println("                    Secondary Weight: " + SQLUtility.formatDecimal3(virtualDisk.secondaryWeight));
              System.out.println("                    Secondary Device: " + virtualDisk.selectedSecondaryDisk.device);
            }
          }
        }
        if (!isFirst) {
          System.out.println("            Total:");
        }
      }
    }
  }

  private static long mapped;
  private static long skipped;

  /**
   * Try all permutations of mappings from virtual server to physical servers, only continuing to the next allocation
   * checks if the total CPU cores, RAM, number of disk arrays, and disk extents is <= what the physical hardware provides.
   * Also only map the permutations when they match minimumProcessorType, requiredProcessorArchitecture, and minimumProcessorSpeed.
   * These are the quick checks that don't need to worry about the actual mappings to specific primary and secondary disk
   * arrays.
   */
  private static void mapVirtualServersToPrimaryServers(VirtualServer[] virtualServers, Host[] hosts, int currentVirtualServer) {
    final int hostsSize = hosts.length;
    final int virtualServersSize = virtualServers.length;

    displayProgress(virtualServers, hosts, currentVirtualServer);

    if (currentVirtualServer == virtualServersSize) {
      mapped++;
      if (TERMINATE_ON_FIRST_MATCH) {
        displayMapping(virtualServers, hosts);
        displaySkipTypes();
        System.exit(0);
      }
    } else {
      final VirtualServer virtualServer = virtualServers[currentVirtualServer];
      // Try each primary server
      for (int primaryServerIndex = 0; primaryServerIndex < hostsSize; primaryServerIndex++) {
        final Host primaryServer = hosts[primaryServerIndex];
        // First allow manual configuration
        if (virtualServer.primaryServerHostname == null || virtualServer.primaryServerHostname.equals(primaryServer.hostname)) {
          // Only map the virtual server to the primary server if it matches any processor type, architecture, and speed constraints.
          if (
              (
                  virtualServer.minimumProcessorType == null
                      || primaryServer.processorType.compareTo(virtualServer.minimumProcessorType) >= 0
              ) && (
                  virtualServer.requiredProcessorArchitecture == null
                      || primaryServer.processorArchitecture == virtualServer.requiredProcessorArchitecture
              ) && (
                  virtualServer.minimumProcessorSpeed == -1
                      || primaryServer.processorSpeed >= virtualServer.minimumProcessorSpeed
              )
          ) {
            // Stop processing if primaryServer past capacity on either processor cores or RAM
            final int oldAllocatedProcessorWeight = primaryServer.allocatedProcessorWeight;
            final int newAllocatedProcessorWeight = oldAllocatedProcessorWeight + virtualServer.processorCores * virtualServer.processorWeight;
            if ((primaryServer.processorCores * 1000) >= newAllocatedProcessorWeight) {
              // Stop processing if primaryServer past capacity on RAM (primary + maximum of the secondaries)
              final int oldAllocatedPrimaryRAM = primaryServer.allocatedPrimaryRAM;
              final int newAllocatedPrimaryRAM = oldAllocatedPrimaryRAM + virtualServer.primaryRam;
              if (primaryServer.ram >= (newAllocatedPrimaryRAM + primaryServer.maximumAllocatedSecondaryRAM)) {
                virtualServers[currentVirtualServer].selectedPrimaryServerIndex = primaryServerIndex;
                primaryServer.allocatedProcessorWeight = newAllocatedProcessorWeight;
                primaryServer.allocatedPrimaryRAM = newAllocatedPrimaryRAM;

                // Try all the combinations of virtualDisk to disk mappings (recursively)
                if (TRACE) {
                  System.out.println(virtualServer.hostname + ": Primary on " + primaryServer.hostname);
                }
                mapVirtualDisksToPrimaryDisks(virtualServers, hosts, currentVirtualServer, virtualServer.virtualDisks, primaryServer.disks, 0);

                virtualServers[currentVirtualServer].selectedPrimaryServerIndex = -1;
                primaryServer.allocatedProcessorWeight = oldAllocatedProcessorWeight;
                primaryServer.allocatedPrimaryRAM = oldAllocatedPrimaryRAM;
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
        } else {
          skipped++;
          SkipType.PRIMARY_MANUAL_SERVER_MISMATCH.counter++;
        }
      }
    }
  }

  private static void mapVirtualDisksToPrimaryDisks(VirtualServer[] virtualServers, Host[] servers, int currentVirtualServer, VirtualDisk[] virtualDisks, Disk[] primaryDisks, int currentVirtualDisk) {
    final int primaryDisksSize = primaryDisks.length;
    final int virtualDisksSize = virtualDisks.length;

    if (currentVirtualDisk == virtualDisksSize) {
      // All virtual disks are mapped to primary server now
      mapVirtualServersToSecondaryServers(virtualServers, servers, currentVirtualServer);
    } else {
      final VirtualDisk virtualDisk = virtualDisks[currentVirtualDisk];

      // Try each primary disk
      for (int primaryDiskIndex = 0; primaryDiskIndex < primaryDisksSize; primaryDiskIndex++) {
        final Disk primaryDisk = primaryDisks[primaryDiskIndex];

        // Skip this primary disk if the type doesn't match
        if (virtualDisk.primaryDiskType == primaryDisk.diskType) {
          // Make sure has enough extents
          final int oldPrimaryDiskAllocatedExtents = primaryDisk.allocatedExtents;
          final int newPrimaryDiskAllocatedExtents = oldPrimaryDiskAllocatedExtents + virtualDisk.extents;
          if (primaryDisk.extents >= newPrimaryDiskAllocatedExtents) {
            final int oldPrimaryDiskAllocatedWeight = primaryDisk.allocatedWeight;
            final int newPrimaryDiskAllocatedWeight = oldPrimaryDiskAllocatedWeight + virtualDisk.primaryWeight;
            if (newPrimaryDiskAllocatedWeight <= 1000) {
              boolean matchesPrevious = false;
              if (!FIND_ALL_PERMUTATIONS) {
                // If remaining weight and extents exactly match a primary disk already checked, skip because there is no effective difference
                for (int previousPrimaryDiskIndex = 0; previousPrimaryDiskIndex < primaryDiskIndex; previousPrimaryDiskIndex++) {
                  final Disk previousPrimaryDisk = primaryDisks[previousPrimaryDiskIndex];
                  if (
                      previousPrimaryDisk.diskType == primaryDisk.diskType
                          && (previousPrimaryDisk.extents - previousPrimaryDisk.allocatedExtents) == (primaryDisk.extents - oldPrimaryDiskAllocatedExtents)
                          && previousPrimaryDisk.allocatedWeight == oldPrimaryDiskAllocatedWeight
                  ) {
                    matchesPrevious = true;
                    break;
                  }
                }
              }
              if (!matchesPrevious) {
                virtualDisk.selectedPrimaryDisk = primaryDisk;
                primaryDisk.allocatedExtents = newPrimaryDiskAllocatedExtents;
                primaryDisk.allocatedWeight = newPrimaryDiskAllocatedWeight;

                mapVirtualDisksToPrimaryDisks(virtualServers, servers, currentVirtualServer, virtualDisks, primaryDisks, currentVirtualDisk + 1);

                virtualDisk.selectedPrimaryDisk = null;
                primaryDisk.allocatedExtents = oldPrimaryDiskAllocatedExtents;
                primaryDisk.allocatedWeight = oldPrimaryDiskAllocatedWeight;
              } else {
                skipped++;
                SkipType.PRIMARY_DISK_MATCHES_PREVIOUS.counter++;
                if (TRACE) {
                  System.out.println("        PRIMARY_DISK_MATCHES_PREVIOUS");
                }
              }
            } else {
              skipped++;
              SkipType.PRIMARY_DISK_WEIGHT_EXCEEDED.counter++;
            }
          } else {
            skipped++;
            SkipType.PRIMARY_DISK_EXTENTS_EXCEEDED.counter++;
            if (TRACE) {
              System.out.println("        PRIMARY_DISK_EXTENTS_EXCEEDED");
            }
          }
        } else {
          skipped++;
          SkipType.PRIMARY_DISK_TYPE_MISMATCH.counter++;
        }
      }
    }
  }

  private static void mapVirtualServersToSecondaryServers(VirtualServer[] virtualServers, Host[] hosts, int currentVirtualServer) {
    final int hostsSize = hosts.length;
    final VirtualServer virtualServer = virtualServers[currentVirtualServer];
    final int primaryServerIndex = virtualServers[currentVirtualServer].selectedPrimaryServerIndex;

    // Now try each of the possible secondary mappings (to all servers except the primary)
    for (int secondaryServerIndex = 0; secondaryServerIndex < hostsSize; secondaryServerIndex++) {
      if (secondaryServerIndex != primaryServerIndex) {
        final Host secondaryServer = hosts[secondaryServerIndex];

        // Allow for manual configuration of secondary
        if (virtualServer.secondaryServerHostname == null || virtualServer.secondaryServerHostname.equals(secondaryServer.hostname)) {
          // Make sure the secondary processorType and architecture match any requirements
          if (
              (
                  virtualServer.minimumProcessorType == null
                      || secondaryServer.processorType.compareTo(virtualServer.minimumProcessorType) >= 0
              ) && (
                  virtualServer.requiredProcessorArchitecture == null
                      || secondaryServer.processorArchitecture == virtualServer.requiredProcessorArchitecture
              )
          ) {
            // Make sure secondary has at least total number of cores matching secondary cores.
            // Note: we don't care about weight here - just make it run somewhere when in failover.
            if (secondaryServer.processorCores >= virtualServer.processorCores) {
              // Make sure that the combined primary mapping plus secondary RAM does not exceed the total of this possible secondary machine
              // for any one primary failure.  The loop represents the failure of each server, one at a time.
              final int oldAllocatedSecondaryRAM = secondaryServer.allocatedSecondaryRAMs[primaryServerIndex];
              final int newAllocatedSecondaryRAM = oldAllocatedSecondaryRAM + virtualServer.secondaryRam;
              if (secondaryServer.ram >= (secondaryServer.allocatedPrimaryRAM + newAllocatedSecondaryRAM)) {
                virtualServers[currentVirtualServer].selectedSecondaryServerIndex = secondaryServerIndex;
                secondaryServer.allocatedSecondaryRAMs[primaryServerIndex] = newAllocatedSecondaryRAM;
                final int oldMaximumAllocatedSecondaryRAM = secondaryServer.maximumAllocatedSecondaryRAM;
                if (newAllocatedSecondaryRAM > oldMaximumAllocatedSecondaryRAM) {
                  secondaryServer.maximumAllocatedSecondaryRAM = newAllocatedSecondaryRAM;
                }

                // Try all the combinations of virtualDisk to disk mappings (recursively)
                if (TRACE) {
                  System.out.println("    " + virtualServer.hostname + ": Secondary on " + secondaryServer.hostname);
                }
                mapVirtualDisksToSecondaryDisks(virtualServers, hosts, currentVirtualServer, virtualServer.virtualDisks, secondaryServer.disks, 0);

                virtualServers[currentVirtualServer].selectedSecondaryServerIndex = -1;
                secondaryServer.allocatedSecondaryRAMs[primaryServerIndex] = oldAllocatedSecondaryRAM;
                secondaryServer.maximumAllocatedSecondaryRAM = oldMaximumAllocatedSecondaryRAM;
              } else {
                skipped++;
                SkipType.SECONDARY_RAM_EXCEEDED.counter++;
              }
            } else {
              skipped++;
              SkipType.SECONDARY_CORES_EXCEEDED.counter++;
              if (TRACE) {
                System.out.println("        SECONDARY_CORES_EXCEEDED: " + secondaryServer.hostname + "." + secondaryServer.processorCores + "<" + virtualServer.hostname + "." + virtualServer.processorCores);
              }
            }
          } else {
            skipped++;
            SkipType.SECONDARY_PROCESSOR_MISMATCH.counter++;
          }
        } else {
          skipped++;
          SkipType.SECONDARY_MANUAL_SERVER_MISMATCH.counter++;
        }
      }
    }
  }

  private static void mapVirtualDisksToSecondaryDisks(VirtualServer[] virtualServers, Host[] servers, int currentVirtualServer, VirtualDisk[] virtualDisks, Disk[] secondaryDisks, int currentVirtualDisk) {
    final int secondaryDisksSize = secondaryDisks.length;
    final int virtualDisksSize = virtualDisks.length;

    if (currentVirtualDisk == virtualDisksSize) {
      // All virtual disks are mapped to secondary server now
      mapVirtualServersToPrimaryServers(virtualServers, servers, currentVirtualServer + 1);
    } else {
      final VirtualDisk virtualDisk = virtualDisks[currentVirtualDisk];

      // Try each secondary disk
      for (int secondaryDiskIndex = 0; secondaryDiskIndex < secondaryDisksSize; secondaryDiskIndex++) {
        final Disk secondaryDisk = secondaryDisks[secondaryDiskIndex];

        // Skip this secondary disk if the type doesn't match
        if (virtualDisk.secondaryDiskType == secondaryDisk.diskType) {
          // Make sure has enough extents
          final int oldSecondaryDiskAllocatedExtents = secondaryDisk.allocatedExtents;
          final int newSecondaryDiskAllocatedExtents = oldSecondaryDiskAllocatedExtents + virtualDisk.extents;
          //System.out.println("oldSecondaryDiskAllocatedExtents="+oldSecondaryDiskAllocatedExtents);
          //System.out.println("newSecondaryDiskAllocatedExtents="+newSecondaryDiskAllocatedExtents);
          if (secondaryDisk.extents >= newSecondaryDiskAllocatedExtents) {
            final int oldSecondaryDiskAllocatedWeight = secondaryDisk.allocatedWeight;
            final int newSecondaryDiskAllocatedWeight = oldSecondaryDiskAllocatedWeight + virtualDisk.secondaryWeight;
            if (newSecondaryDiskAllocatedWeight <= 1000) {
              boolean matchesPrevious = false;
              if (!FIND_ALL_PERMUTATIONS) {
                // If remaining weight and extents exactly match a secondary disk already checked, skip because there is no effective difference
                for (int previousSecondaryDiskIndex = 0; previousSecondaryDiskIndex < secondaryDiskIndex; previousSecondaryDiskIndex++) {
                  final Disk previousSecondaryDisk = secondaryDisks[previousSecondaryDiskIndex];
                  if (
                      previousSecondaryDisk.diskType == secondaryDisk.diskType
                          && (previousSecondaryDisk.extents - previousSecondaryDisk.allocatedExtents) == (secondaryDisk.extents - oldSecondaryDiskAllocatedExtents)
                          && previousSecondaryDisk.allocatedWeight == oldSecondaryDiskAllocatedWeight
                  ) {
                    matchesPrevious = true;
                    break;
                  }
                }
              }
              if (!matchesPrevious) {
                virtualDisk.selectedSecondaryDisk = secondaryDisk;
                secondaryDisk.allocatedExtents = newSecondaryDiskAllocatedExtents;
                secondaryDisk.allocatedWeight = newSecondaryDiskAllocatedWeight;

                mapVirtualDisksToSecondaryDisks(virtualServers, servers, currentVirtualServer, virtualDisks, secondaryDisks, currentVirtualDisk + 1);

                virtualDisk.selectedSecondaryDisk = null;
                secondaryDisk.allocatedExtents = oldSecondaryDiskAllocatedExtents;
                secondaryDisk.allocatedWeight = oldSecondaryDiskAllocatedWeight;
              } else {
                skipped++;
                SkipType.SECONDARY_DISK_MATCHES_PREVIOUS.counter++;
              }
            } else {
              skipped++;
              SkipType.SECONDARY_DISK_WEIGHT_EXCEEDED.counter++;
            }
          } else {
            skipped++;
            SkipType.SECONDARY_DISK_EXTENTS_EXCEEDED.counter++;
            if (TRACE) {
              System.out.println("        SECONDARY_DISK_EXTENTS_EXCEEDED: " + secondaryDisk.extents + "<" + newSecondaryDiskAllocatedExtents);
            }
          }
        } else {
          skipped++;
          SkipType.SECONDARY_DISK_TYPE_MISMATCH.counter++;
          if (TRACE) {
            System.out.println("        SECONDARY_DISK_TYPE_MISMATCH: " + virtualDisk.secondaryDiskType + " != " + secondaryDisk.diskType);
          }
        }
      }
    }
  }
}
