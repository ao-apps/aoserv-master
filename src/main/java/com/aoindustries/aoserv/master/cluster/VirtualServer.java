/*
 * aoserv-master - Master server for the AOServ Platform.
 * Copyright (C) 2007-2013, 2020, 2021  AO Industries, Inc.
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
 * along with aoserv-master.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.aoserv.master.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author  AO Industries, Inc.
 */
@SuppressWarnings("overrides") // We will not implement hashCode, despite having equals
public final class VirtualServer implements Comparable<VirtualServer> {

	/**
	 * TODO: Load this directly from the database.
	 */
	static VirtualServer[] getVirtualServers() {
		List<VirtualServer> virtualServers = new ArrayList<>();
		virtualServers.add(
			new VirtualServer(
				"ao1.kc.aoindustries.com",
				null,
				null,
				1024,
				512,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 62)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"centos5.aoindustries.com",
				null,
				null,
				256,
				0,
				ProcessorType.XEON_LV,
				null,
				-1,
				2,
				31,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 62, DiskType.RAID1_7200, 16)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"centos5-build64.aoindustries.com",
				null,
				null,
				256,
				0,
				ProcessorType.XEON_LV,
				null,
				-1,
				2,
				31,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 62, DiskType.RAID1_7200, 16)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"daissystems.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 4480+896, DiskType.RAID1_7200, 500, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"ipharos.com",
				"xen907-1.fc.aoindustries.com",
				"xen917-5.fc.aoindustries.com",
				4096,
				0, // Need 2048, Desire 4096
				null,
				null,
				-1,
				4,
				500, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792+896, DiskType.RAID1_15000, 500, DiskType.RAID1_15000, 500)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"db1.fc.ipharos.com",
				"xen907-1.fc.aoindustries.com",
				"xen917-5.fc.aoindustries.com",
				2048, // Need 4096
				0, // Need 4096
				null,
				null,
				-1,
				4,
				250, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_15000, 500, DiskType.RAID1_15000, 500) // Need to be 1792, .5, .5 once ipharos.com is gone - and secondary on 15k
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.fc.ipharos.com",
				"xen917-5.fc.aoindustries.com",
				null, // "xen907-1.fc.aoindustries.com",
				2048, // Need 4096
				0, // Need 4096
				null,
				null,
				-1,
				4,
				250, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896+896, DiskType.RAID1_7200, 1000, DiskType.RAID1_7200, 1000)
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
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"ns1.aoindustries.com",
				null, //"xen914-5.fc.lnxhosting.ca",
				null,
				256,
				256,
				null,
				null,
				-1,
				2,
				125,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 31)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"ns4.aoindustries.com",
				null, //"xen917-5.fc.aoindustries.com",
				null,
				256,
				256,
				null,
				null,
				-1,
				2,
				125,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 31)
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
				2,
				125, // Desire 500,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1000, DiskType.RAID1_7200, 250)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.limlom.com",
				"xen917-5.fc.aoindustries.com",
				null,
				2048,
				0,  // Need 2048
				null,
				null,
				-1,
				1,
				250, // Desire 1000,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.fc.enduraquest.com",
				"xen917-4.fc.aoindustries.com",
				null,
				4096,
				2048, // Desire 4096
				null,
				null,
				-1,
				2,
				1000,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_10000, 1000, DiskType.RAID1_7200, 250)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.fc.lnxhosting.ca",
				"xen914-5.fc.lnxhosting.ca",
				null,
				4096,
				4096,
				null,
				ProcessorArchitecture.X86_64,
				-1,
				4,
				500,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 62)
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
				125, // Desire 250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 62),
					new VirtualDisk("/dev/xvdb", 8064+896*2, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 62)
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
				1000, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 500, DiskType.RAID1_7200, 250)
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
				2,
				250, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896+896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
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
				2,
				125, // Desire 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
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
				2,
				125, // Desire 250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
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
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 3584+896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
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
				2,
				250, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.nl.pertinence.net",
				null,
				null,
				2048,
				1024,
				null,
				null,
				-1,
				4,
				125, // Desire 250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 4480+896*2, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125) // Need more space
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
				1000,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 1000, DiskType.RAID1_7200, 500)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www2.kc.aoindustries.com",
				null,
				null,
				1536, // Had solution at 1024
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125) // Desire more space
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www3.kc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				125,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www4.kc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www5.kc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www6.kc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www7.fc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				125,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792, DiskType.RAID1_7200, 250, DiskType.RAID1_7200, 125) // Desire more space
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www8.kc.aoindustries.com",
				null,
				null,
				2048,
				2048,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 2688+896, DiskType.RAID1_7200, 500, DiskType.RAID1_7200, 250)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www9.fc.aoindustries.com",
				null,
				null,
				1024,
				1024,
				null,
				null,
				-1,
				2,
				250,
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 1792+896, DiskType.RAID1_7200, 500, DiskType.RAID1_7200, 250)
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
				750, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 7450*2, DiskType.RAID1_7200, 1000, DiskType.RAID1_7200, 500) // TODO: Estimated size
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
				-1,
				2,
				250, // Need 1000
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 2688, DiskType.RAID1_7200, 500, DiskType.RAID1_7200, 250)
				}
			)
		);
		virtualServers.add(
			new VirtualServer(
				"www1.fc.everylocalad.com",
				null,
				null,
				256,
				256,
				null,
				null,
				-1,
				2,
				250, // Need 500
				new VirtualDisk[] {
					new VirtualDisk("/dev/xvda", 896, DiskType.RAID1_7200, 125, DiskType.RAID1_7200, 32)
				}
			)
		);
		Collections.sort(virtualServers);
		// Collections.shuffle(virtualServers);
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
	@Override
	public int compareTo(VirtualServer other) {
		/*
		return -realCompareTo(other);
	}

	public int realCompareTo(VirtualServer other) {
		 */
		// Shortcut for identity
		if(this==other) return 0;
		// By manual configuration first
		if(primaryServerHostname!=null) {
			if(other.primaryServerHostname==null) return -1;
			int diff = primaryServerHostname.compareTo(other.primaryServerHostname);
			if(diff!=0) return diff;
		} else {
			if(other.primaryServerHostname!=null) return 1;
		}
		if(secondaryServerHostname!=null) {
			if(other.secondaryServerHostname==null) return -1;
			int diff = secondaryServerHostname.compareTo(other.secondaryServerHostname);
			if(diff!=0) return diff;
		} else {
			if(other.secondaryServerHostname!=null) return 1;
		}
		// By RAM
		if(primaryRam<other.primaryRam) return 1;
		if(primaryRam>other.primaryRam) return -1;
		if(secondaryRam<other.secondaryRam) return 1;
		if(secondaryRam>other.secondaryRam) return -1;
		// By Processor Cores
		int diff = (other.processorCores*other.processorWeight) - (processorCores*processorWeight);
		if(diff!=0) return diff;
		// By Disk Extents
		long totalExtents = 0;
		for(VirtualDisk virtualDisk : virtualDisks) totalExtents += virtualDisk.extents;
		long otherTotalExtents = 0;
		for(VirtualDisk virtualDisk : other.virtualDisks) otherTotalExtents += virtualDisk.extents;
		if(totalExtents<otherTotalExtents) return -1;
		if(totalExtents>otherTotalExtents) return 1;
		/*
		 * TODO: Add rest to be consistent with equals
		if(minimumProcessorType!=other.minimumProcessorType) return false;
		if(requiredProcessorArchitecture!=other.requiredProcessorArchitecture) return false;
		if(minimumProcessorSpeed!=other.minimumProcessorSpeed) return false;
		if(processorCores!=other.processorCores) return false;
		if(processorWeight!=other.processorWeight) return false;
		if(virtualDisks.length!=other.virtualDisks.length) return false;
		for(int c=0;c<virtualDisks.length;c++) {
			if(!virtualDisks[c].equals(other.virtualDisks[c])) return false;
		}
		 */
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj instanceof VirtualServer) && equals((VirtualServer)obj);
	}

	public boolean equals(VirtualServer other) {
		return other!=null && compareTo(other)==0;
		/*
		// Shortcut for identity
		if(this==other) return true;
		if(primaryRam!=other.primaryRam) return false;
		if(secondaryRam!=other.secondaryRam) return false;
		if(minimumProcessorType!=other.minimumProcessorType) return false;
		if(requiredProcessorArchitecture!=other.requiredProcessorArchitecture) return false;
		if(minimumProcessorSpeed!=other.minimumProcessorSpeed) return false;
		if(processorCores!=other.processorCores) return false;
		if(processorWeight!=other.processorWeight) return false;
		if(virtualDisks.length!=other.virtualDisks.length) return false;
		if(!Strings.equals(primaryServerHostname, other.primaryServerHostname)) return false;
		if(!Strings.equals(secondaryServerHostname, other.secondaryServerHostname)) return false;
		for(int c=0;c<virtualDisks.length;c++) {
			if(!virtualDisks[c].equals(other.virtualDisks[c])) return false;
		}
		return true;
		 */
	}

	public static void main(String[] args) {
		VirtualServer[] virtualServers = getVirtualServers();
		for(VirtualServer y : virtualServers) {
			for(VirtualServer x : virtualServers) {
				System.out.print(x.equals(y) ? '@' : '-');
			}
			System.out.println();
		}
	}
}
