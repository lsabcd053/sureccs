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

import java.util.*;

/* Class for keeping track of under replication blocks
 * Blocks have replication priority, with priority 0 indicating the highest
 * Blocks have only one replicas has the highest
 */
class RSGroupW {
	private RSGroup group;
	private int red;
	
	public RSGroupW(RSGroup grp) {
		group = grp;
		red = grp.getN() - grp.getM();
	}
	
	public void reduce()
	{
		if(red > 0)
			red--;
	}
	
	public RSGroup getGroup()
	{
		return group;
	}
	
	public int getRed()
	{
		return red;
	}
}
class UnderEncodedGroups{
	private List<RSGroupW> groupQueues = new ArrayList<RSGroupW>();

	/* constructor */
	UnderEncodedGroups() {
	}

	/**
	 * Empty the queues.
	 */
	void clear() {
		groupQueues.clear();
	}

	/* Return the total number of under encoded groups */
	synchronized int size() {
		return groupQueues.size();
	}

	/* Check if a group is in the underEncoded queue */
	synchronized RSGroupW contains(RSGroup grp) {
		for (RSGroupW groupw : groupQueues) {
			if (groupw.getGroup().isGrouptheSame(grp)) {
				return groupw;
			}
		}
		return null;
	}

	/*
	 * add a group to a under encoded queue
	 * 
	 * @param group The group to be encoded
	 */
	synchronized void add(RSGroup group) {
		if(this.contains(group) == null)
			groupQueues.add(new RSGroupW(group));
	}

	synchronized void remove(RSGroup group, boolean encoding) {
		RSGroupW grpw = this.contains(group);
		if(grpw != null){
			if (!encoding) {
				if (grpw.getRed() > 0)
					grpw.reduce();
				else
					groupQueues.remove(grpw);
			} else {
				groupQueues.remove(grpw);
			}
		}
	}
	
	public List<RSGroupW> getUnderEncodedGroups(){
		return groupQueues;
	}
}
