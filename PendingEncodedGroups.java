/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.dfs;

import org.apache.hadoop.dfs.PendingReplicationBlocks.PendingBlockInfo;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;
import java.sql.Time;

/***************************************************
 * PendingEncodedGroups does the bookkeeping of all groups that are getting
 * encoded.
 * 
 ***************************************************/
class PendingEncodedGroups {
	private List<RSGroup> pendingEncodedGroups;
	private Map<Block, PendingBlockInfo> pendingReplications;
	private ArrayList<RSGroup> timedOutItems;
	Daemon timerThread = null;
	private volatile boolean fsRunning = true;

	//
	// It might take anywhere between 5 to 10 minutes before
	// a request is timed out.
	//
	private long timeout = 10 * 60 * 1000;
	private long defaultRecheckInterval = 10 * 60 * 1000;

	PendingEncodedGroups(long timeoutPeriod) {
		if (timeoutPeriod > 0) {
			this.timeout = timeoutPeriod;
		}
		init();
	}

	PendingEncodedGroups() {
		init();
	}

	void init() {
		pendingEncodedGroups = new ArrayList<RSGroup>();
		pendingReplications = new HashMap<Block, PendingBlockInfo>();
		timedOutItems = new ArrayList<RSGroup>();
		this.timerThread = new Daemon(new PendingEncodedGroupsMonitor());
		timerThread.start();
	}

	/**
	 * Add a group to the list of pending encoded groups and add relative
	 * redundant block to the list of pending Replications
	 */
	void add(RSGroup group, int numReplicas) {
		synchronized (pendingEncodedGroups) {
			boolean found = pendingEncodedGroups.contains(group);
			if (found == false) {
				pendingEncodedGroups.add(group);
				Block[] cBlocks = group.getCodingBlocks();
				for (int i = 0; i < cBlocks.length; i++) {
					pendingReplications.put(cBlocks[i], new PendingBlockInfo(
							numReplicas));
				}
			}
		}
	}

	/**
	 * One replication request for this block has finished. Decrement the number
	 * of pending replication requests for this block.
	 */
	void remove(RSGroup group) {
		int completeCount = 0;
		int red = group.getN() - group.getM();
		Block[] cBlocks = group.getCodingBlocks();
		PendingBlockInfo[] found = new PendingBlockInfo[cBlocks.length];
		synchronized (pendingEncodedGroups) {
			for (int i = 0; i < cBlocks.length; i++) {
				synchronized (pendingReplications) {
					found[i] = pendingReplications.get(cBlocks[i]);
					if (found[i] != null) {
						found[i].decrementReplicas();
						if (found[i].getNumReplicas() <= 0) {
							// pendingReplications.remove(block);
							completeCount++;
						}
					}
				}
			}
			if (completeCount == red) {
				pendingEncodedGroups.remove(group);
				for (int i = 0; i < cBlocks.length; i++) {
					synchronized (pendingReplications) {
						pendingReplications.remove(cBlocks[i]);
					}
				}
			}
		}
	}

	/**
	 * The total number of blocks that are undergoing replication
	 */
	int size() {
		return pendingEncodedGroups.size();
	}

	/**
	 * How many copies of this block is pending replication?
	 */
	int getNumReplicas(Block block) {
		synchronized (pendingReplications) {
			PendingBlockInfo found = pendingReplications.get(block);
			if (found != null) {
				return found.getNumReplicas();
			}
		}
		return 0;
	}

	/**
	 * Returns a list of groups that have timed out their encoding requests.
	 * Returns null if no groups have timed out.
	 */
	RSGroup[] getTimedOutGroups() {
		synchronized (timedOutItems) {
			if (timedOutItems.size() <= 0) {
				return null;
			}
			RSGroup[] groupList = timedOutItems
					.toArray(new RSGroup[timedOutItems.size()]);
			timedOutItems.clear();
			return groupList;
		}
	}

	/*
	 * A periodic thread that scans for blocks that never finished their
	 * replication request.
	 */
	class PendingEncodedGroupsMonitor implements Runnable {
		public void run() {
			while (fsRunning) {
				long period = Math.min(defaultRecheckInterval, timeout);
				try {
					pendingEncodingGroupsCheck();
					Thread.sleep(period);
				} catch (InterruptedException ie) {
					FSNamesystem.LOG
							.debug("PendingReplicationMonitor thread received exception. "
									+ ie);
				}
			}
		}

		/**
		 * Iterate through all items and detect timed-out items
		 */
		void pendingEncodingGroupsCheck() {
			synchronized (pendingEncodedGroups) {
				Iterator iter = pendingEncodedGroups.iterator();
				long now = FSNamesystem.now();
				FSNamesystem.LOG
						.debug("PendingEncodingGroupsMonitor checking.");
				while (iter.hasNext()) {
					// Map.Entry entry = (Map.Entry) iter.next();
					RSGroup group = (RSGroup) iter.next();
					Block[] blocks = group.getCodingBlocks();
					PendingBlockInfo[] pendingBlock = new PendingBlockInfo[blocks.length];
					for (int i = 0; i < blocks.length; i++) {
						synchronized (pendingReplications) {
							pendingBlock[i] = pendingReplications.get(blocks[i]);
						}
						if (now > pendingBlock[i].getTimeStamp() + timeout) {
							// Block block = (Block) entry.getKey();
							synchronized (timedOutItems) {
								timedOutItems.add(group);
							}
							FSNamesystem.LOG
									.warn("PendingReplicationMonitor timed out block "
											+ blocks[i]);
							iter.remove();
							for(int j = 0; j < blocks.length; j++){
								pendingReplications.remove(blocks[j]);
							}
							break;
						}
					}
				}
			}
		}
	}

	/*
	 * Shuts down the pending replication monitor thread. Waits for the thread
	 * to exit.
	 */
	void stop() {
		fsRunning = false;
		timerThread.interrupt();
		try {
			timerThread.join(3000);
		} catch (InterruptedException ie) {
		}
	}

}
