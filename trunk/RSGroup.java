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

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.dfs.BlocksMap.BlockInfo;

/**************************************************
 * Group is a set for various blocks, actually this just for coding. You know, a
 * group of block will be used for RS Code, which is important in failure
 * tolerance. First edition implemented by Robeen,it's reference comes to class
 * Block
 * 
 **************************************************/

class RSGroup implements Writable{
	
	static { // register a ctor
		WritableFactories.setFactory(RSGroup.class, new WritableFactory() {
			public Writable newInstance() {
				return new RSGroup();
			}
		});
	}

	private int groupID; // The only identification for group
	private int szGroup; // Size of a group to verify the total
	// TODO Add groupid here
	private int couldBeCoded; // This could be used to verify that if it has the

	// ability to recover when all the replicas are broken,
	// That is the code ability

	private BlockInfo blocks[] = null;
	
	// ///////////////////////////////////
	// Writable To support serialization, we should write a block to disc
	// ///////////////////////////////////
	public void write(DataOutput out) throws IOException {
		out.writeInt(groupID);
		out.writeInt(szGroup);
		out.writeInt(couldBeCoded);
		out.writeInt(blocks.length);
		for(int i = 0; i < blocks.length; i++)
		{
			blocks[i].write(out);
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.groupID = in.readInt();
		this.szGroup = in.readInt();
		this.couldBeCoded = in.readInt();
		this.blocks = new BlockInfo[in.readInt()];
		for(int i = 0; i < blocks.length; i++)
		{
			blocks[i].readFields(in);
		}
		if (szGroup < 0) {
			throw new IOException("Unexpected Group size: " + szGroup);
		}
	}

	public RSGroup() {
		groupID = 0;
		szGroup = 0;
		blocks = null;
		couldBeCoded = 1;
	}

	public RSGroup(int gID, int size) {
		this.set(gID, size);
		blocks = null;
		couldBeCoded = 1;
	}

	public void set(int gID, int size) {
		groupID = gID;
		szGroup = size;
	}

	public int getGroupId() {
		return groupID;
	}

	public int getSizeGroup() {
		return szGroup;
	}

	/**
	 * add a block to the group
	 * @throws IOException 
	 */
	public void addBlock(BlockInfo newblock) throws IOException {
		// TODO SUR_ECCS.log <function:"Add newBlock "+newBlock+" to group "+this.toString()>
		FileWriter log = new FileWriter("SUR_ECCS.log", true);
		String s = "At RSGroup.java, RSGroup.addBlock,"+
			"<function:Add newBlock " +
			newblock + 
			" to group " +
			this.toString()+">\n";
		log.write(s);
		log.close();
		
		if (this.blocks == null) {
			this.blocks = new BlockInfo[1];
			this.blocks[0] = newblock;
		} else {
			int size = this.blocks.length;
			BlockInfo[] newlist = new BlockInfo[size + 1];
			for (int i = 0; i < size; i++) {
				newlist[i] = this.blocks[i];
			}
			newlist[size] = newblock;
			this.blocks = newlist;
		}
	}

	public BlockInfo[] getBlocks() {
		return this.blocks;
	}
	
	public void setBlock(int idx, BlockInfo block)
	{
		this.blocks[idx] = block;
	}

	/*
	 * ȷ����ǰblock��group��
	 * @BlockInfo block����Ҫ��֤��block
	 * @return������ڣ����ص�ǰ��group��groupID>=0�����򷵻�-1
	 */
	public int getGroupfromBlock(BlockInfo block) throws IOException {
		// TODO SUR_ECCS.log <function:"Make sure block "+block+" is in the group "+this.toString()>
		FileWriter log = new FileWriter("SUR_ECCS.log", true);
		String s = "At RSGroup.java, RSGroup.getGroupfromBlock,"+
			"<function:Make sure block " +
			block + 
			" is in the group " +
			this.toString()+">\n";
		log.write(s);
		log.close();
		
		if(this.blocks == null)
			return -1;
		int size = this.blocks.length;
		int i = 0;
		for (; i < size; i++) {
			if (blocks[i].getBlockId() == block.getBlockId()) {
				break;
			}
		}
		if (i == size) {
			return -1; // That means the specified block isn't here
		}
		return groupID;
	}

	/* 
	 *  ��ȡ��ǰgoup�����ݣ�����group��ƣ�group����block
	 *  The groupName can be read just like this:
	 *  grp_"groupId":(blk_BlockId1, blk_BlockId2,...);
	 *  @return������groupName
	 */
	public String getGroupName() {
		String rValue = ("grp_" + String.valueOf(groupID));
		int size = this.blocks.length;
		rValue += ":(";
		for (int i = 0; i < size; i++) {
			rValue += this.blocks[i];
			rValue += ",";
		}
		rValue += ")";
		return rValue;
	}

	public String toString() {
		return "grp_" + String.valueOf(groupID);
	}

	public int getBlockSize() {
		return this.blocks.length;
	}
	
	// TODO Add the method to handle groupid and couldBeCoded
	public int couldBeCode() {
		return this.couldBeCoded;
	}

	public void setUnableToCode() {
		this.couldBeCoded = 0;
	}
	
}