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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.dfs.BlocksMap.BlockInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode, such as available
 * storage capacity, last update time, etc., and maintains a set of blocks
 * stored on the datanode.
 * 
 * This data structure is a data structure that is internal to the namenode. It
 * is *not* sent over-the-wire to the Client or the Datnodes. Neither is it
 * stored persistently in the fsImage.
 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {
	
	/** Block and targets pair */
	static class BlockTargetPair {
		final Block block;
		final DatanodeDescriptor[] targets;

		BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
			this.block = block;
			this.targets = targets;
		}
	}

	// TODO: add inner class BlockSrcTargetPair which is used to record the sources node that data come from
	/** Block,sources and targets pair */
	static class BlockSrcTargetPair {
		final Block[] blocks;
		// Hear sources stands for all the possible sources of blocks in a RSGroup,
		// which could be used to encode the redundant blocks or decode the broken blocks 
		// if possible. The targets are originally designed to be a bi-dimensional array because 
		// for an encoding process, the erasure encoded blocks could up to (RS.n - RS.m)
		// and each of the encoded blocks should possibly have multiple targets if r > 1.
		// While for an decoding process, the targets could be considered as one dimension.
		// Here we changes the targets to one dimension for a insistence with the hadoop API.
		// Then for an encoding process, the targets could be identified with 
		// targets[r*i], and traverse i with (RS.n - RS.m ) times 
		final DatanodeDescriptor[] sources;	
		final DatanodeDescriptor[][] targets;
		// Index to confirm the position of the specified block to be decoded
		final int index;
		final RSGroup group;
		
		BlockSrcTargetPair(Block[] block, DatanodeDescriptor[] srcs,
				DatanodeDescriptor[][] targets, int idx, RSGroup grp) {
			this.blocks = block;
			this.sources = srcs;
			this.targets = targets;
			this.index = idx; 
			this.group = grp;
		}
		
		// This 5 simple get function is used for debut log
		
		public Block[] getBlocks()
		{
			return blocks;
		}
		
		public DatanodeDescriptor[] getSources()
		{
			return sources;
		}

		public DatanodeDescriptor[][] getTargets()
		{
			return targets;
		}
		
		public RSGroup getGroup()
		{
			return group;
		}
		
		public int getIdx()
		{
			return index;
		}
	}
	

	/** A BlockTargetPair queue. */
	private static class BlockQueue {
		private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

		/** Size of the queue */
		synchronized int size() {
			return blockq.size();
		}

		/** Enqueue */
		synchronized boolean offer(Block block, DatanodeDescriptor[] targets) {
			return blockq.offer(new BlockTargetPair(block, targets));
		}

		/** Dequeue */
		synchronized List<BlockTargetPair> poll(int numTargets) {
			if (numTargets <= 0 || blockq.isEmpty()) {
				return null;
			} else {
				List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
				for (; !blockq.isEmpty() && numTargets > 0;) {
					numTargets -= blockq.peek().targets.length;
					if (numTargets >= 0) {
						results.add(blockq.poll());
					}
				}
				return results;
			}
		}
	}

	// TODO add the new Block List here to support coding List

	private static class BlockCodingQueue {
		private final Queue<BlockSrcTargetPair> blockcq = new LinkedList<BlockSrcTargetPair>();

		/** Size of the queue */
		synchronized int size() {
			return blockcq.size();
		}

		/** Enqueue */
		synchronized boolean offer(Block[] blocks, DatanodeDescriptor[] srcs,
				DatanodeDescriptor[][] targets, int idx, RSGroup grp) {
			return blockcq.offer(new BlockSrcTargetPair(blocks, srcs, targets, idx, grp));
		}

		/** Dequeue */
		synchronized BlockSrcTargetPair poll() {
			if (blockcq.isEmpty()) {
				return null;
			} else {
				return blockcq.poll();
			}
		}
	}
	
	//TODO Add Complete

	private volatile BlockInfo blockList = null;
	// isAlive == heartbeats.contains(this)
	// This is an optimization, because contains takes O(n) time on Arraylist
	protected boolean isAlive = false;

	/** A queue of blocks to be replicated by this datanode */
	private BlockQueue replicateBlocks = new BlockQueue();
	/** A queue of blocks to be recovered by this datanode */
	private BlockQueue recoverBlocks = new BlockQueue();
	/** A set of blocks to be invalidated by this datanode */
	private Set<Block> invalidateBlocks = new TreeSet<Block>();

	// TODO add a coding List for RS encoding and decoding process
	private BlockCodingQueue encodingBlocks = new BlockCodingQueue();	
	private BlockCodingQueue decodingBlocks = new BlockCodingQueue();
	// TODO 
	
	boolean processedBlockReport = false;

	/*
	 * Variables for maintaning number of blocks scheduled to be written to this
	 * datanode. This count is approximate and might be slightly higher in case
	 * of errors (e.g. datanode does not report if an error occurs while writing
	 * the block).
	 */
	private int currApproxBlocksScheduled = 0;
	private int prevApproxBlocksScheduled = 0;
	private long lastBlocksScheduledRollTime = 0;
	private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600 * 1000; // 10min

	/** Default constructor */
	public DatanodeDescriptor() {
	}

	/**
	 * DatanodeDescriptor constructor
	 * 
	 * @param nodeID
	 *            id of the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID) {
		this(nodeID, 0L, 0L, 0L, 0);
	}

	/**
	 * DatanodeDescriptor constructor
	 * 
	 * @param nodeID
	 *            id of the data node
	 * @param networkLocation
	 *            location of the data node in network
	 */
	public DatanodeDescriptor(DatanodeID nodeID, String networkLocation) {
		this(nodeID, networkLocation, null);
	}

	/**
	 * DatanodeDescriptor constructor
	 * 
	 * @param nodeID
	 *            id of the data node
	 * @param networkLocation
	 *            location of the data node in network
	 * @param hostName
	 *            it could be different from host specified for DatanodeID
	 */
	public DatanodeDescriptor(DatanodeID nodeID, String networkLocation,
			String hostName) {
		this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
	}

	/**
	 * DatanodeDescriptor constructor
	 * 
	 * @param nodeID
	 *            id of the data node
	 * @param capacity
	 *            capacity of the data node
	 * @param dfsUsed
	 *            space used by the data node
	 * @param remaining
	 *            remaing capacity of the data node
	 * @param xceiverCount
	 *            # of data transfers at the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID, long capacity, long dfsUsed,
			long remaining, int xceiverCount) {
		super(nodeID);
		updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
	}

	/**
	 * DatanodeDescriptor constructor
	 * 
	 * @param nodeID
	 *            id of the data node
	 * @param networkLocation
	 *            location of the data node in network
	 * @param capacity
	 *            capacity of the data node, including space used by non-dfs
	 * @param dfsUsed
	 *            the used space by dfs datanode
	 * @param remaining
	 *            remaing capacity of the data node
	 * @param xceiverCount
	 *            # of data transfers at the data node
	 */
	public DatanodeDescriptor(DatanodeID nodeID, String networkLocation,
			String hostName, long capacity, long dfsUsed, long remaining,
			int xceiverCount) {
		super(nodeID, networkLocation, hostName);
		updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
	}

	/**
	 * Add data-node to the block. Add block to the head of the list of blocks
	 * belonging to the data-node.
	 */
	boolean addBlock(BlockInfo b) {
		if (!b.addNode(this))
			return false;
		// add to the head of the data-node list
		blockList = b.listInsert(blockList, this);
		return true;
	}

	/**
	 * Remove block from the list of blocks belonging to the data-node. Remove
	 * data-node from the block.
	 */
	boolean removeBlock(BlockInfo b) {
		blockList = b.listRemove(blockList, this);
		return b.removeNode(this);
	}

	/**
	 * Move block to the head of the list of blocks belonging to the data-node.
	 */
	void moveBlockToHead(BlockInfo b) {
		blockList = b.listRemove(blockList, this);
		blockList = b.listInsert(blockList, this);
	}

	void resetBlocks() {
		this.capacity = 0;
		this.remaining = 0;
		this.xceiverCount = 0;
		this.blockList = null;
		this.invalidateBlocks.clear();
	}

	int numBlocks() {
		return blockList == null ? 0 : blockList.listCount(this);
	}

	/**
   */
	void updateHeartbeat(long capacity, long dfsUsed, long remaining,
			int xceiverCount) {
		this.capacity = capacity;
		this.dfsUsed = dfsUsed;
		this.remaining = remaining;
		this.lastUpdate = System.currentTimeMillis();
		this.xceiverCount = xceiverCount;
		rollBlocksScheduled(lastUpdate);
	}

	/**
	 * Iterates over the list of blocks belonging to the data-node.
	 */
	static private class BlockIterator implements Iterator<Block> {
		private BlockInfo current;
		private DatanodeDescriptor node;

		BlockIterator(BlockInfo head, DatanodeDescriptor dn) {
			this.current = head;
			this.node = dn;
		}

		public boolean hasNext() {
			return current != null;
		}

		public BlockInfo next() {
			BlockInfo res = current;
			current = current.getNext(current.findDatanode(node));
			return res;
		}

		public void remove() {
			throw new UnsupportedOperationException("Sorry. can't remove.");
		}
	}

	Iterator<Block> getBlockIterator() {
		return new BlockIterator(this.blockList, this);
	}

	/**
	 * Store block replication work.
	 */
	void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
		assert (block != null && targets != null && targets.length > 0);
		replicateBlocks.offer(block, targets);
	}
	
	//TODO 
	/**
	 * Store block Encoding work.
	 */
	void addBlockToBeEncoded(Block[] blocks, DatanodeDescriptor[] srcs, DatanodeDescriptor[][] targets, RSGroup grp) {
		assert (blocks != null && srcs != null && srcs.length > 0&& targets != null && targets.length > 0);
		encodingBlocks.offer(blocks, srcs, targets, -1, grp); // For encoding tasks, the index is useless
	}
	//TODO
	
	//TODO 
	/**
	 * Store block Decoding work.
	 */
	void addBlockToBeDecoded(Block block, DatanodeDescriptor[] srcs, DatanodeDescriptor[] targets, int idx, RSGroup grp) {
		assert (block != null && srcs != null && srcs.length > 0&& targets != null && targets.length > 0
				  && idx >= 0 && idx < srcs.length);
		Block[] blocks = new Block[1];
		blocks[0] = block;
		DatanodeDescriptor[][] tars = new DatanodeDescriptor[1][];
		tars[0] = targets;
		decodingBlocks.offer(blocks, srcs, tars, idx, grp);
	}
	//TODO

	/**
	 * Store block recovery work.
	 */
	void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
		assert (block != null && targets != null && targets.length > 0);
		recoverBlocks.offer(block, targets);
	}

	/**
	 * Store block invalidation work.
	 */
	void addBlocksToBeInvalidated(List<Block> blocklist) {
		assert (blocklist != null && blocklist.size() > 0);
		synchronized (invalidateBlocks) {
			for (Block blk : blocklist) {
				invalidateBlocks.add(blk);
			}
		}
	}

	/**
	 * The number of work items that are pending to be replicated
	 */
	int getNumberOfBlocksToBeReplicated() {
		return replicateBlocks.size();
	}

	/**
	 * The number of block invalidation items that are pending to be sent to the
	 * datanode
	 */
	int getNumberOfBlocksToBeInvalidated() {
		synchronized (invalidateBlocks) {
			return invalidateBlocks.size();
		}
	}

	/**
	 * Set the bit signifying that the first block report from this datanode has
	 * been processed
	 */
	void setBlockReportProcessed(boolean val) {
		processedBlockReport = val;
	}

	/**
	 * Have we processed any block report from this datanode yet
	 */
	boolean getBlockReportProcessed() {
		return processedBlockReport;
	}

	BlockCommand getReplicationCommand(int maxTransfers) {
		List<BlockTargetPair> blocktargetlist = replicateBlocks
				.poll(maxTransfers);
		// TODO for test
		String s = "At DatanodeDescriptor.java, in the func: getReplicationCommand";
		if (blocktargetlist != null) {
			Debug.writeTime();
			Debug.writeDebug(s);
			for (int i = 0; i < blocktargetlist.size(); i++) {
				Debug.writeDebug("The node:" + this.toString()
						+ " get a replication command.");
				Debug.writeDebug("It will replicate the block:"
						+ blocktargetlist.get(i).block + " to the targets:");
				DatanodeDescriptor[] targets = blocktargetlist.get(i).targets;
				for (int j = 0; j < targets.length; j++) {
					Debug.writeDebug(targets[j].toString());
				}
			}
		}
		return blocktargetlist == null ? null : new BlockCommand(
				DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
	}

	BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
		List<BlockTargetPair> blocktargetlist = recoverBlocks
				.poll(maxTransfers);
		return blocktargetlist == null ? null : new BlockCommand(
				DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
	}

	/**
	 * Remove the specified number of blocks to be invalidated
	 */
	BlockCommand getInvalidateBlocks(int maxblocks) {
		Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks);
		return deleteList == null ? null : new BlockCommand(
				DatanodeProtocol.DNA_INVALIDATE, deleteList);
	}
	
	// TODO return coding command 
	// Each time we only conduct an encoding/decoding command
	BlockCommand getEncodingCommand() {
		
		BlockSrcTargetPair p  = encodingBlocks.poll();		
		if(p == null)
		{
			//Debug.writeDebug("Half return because there are no encoding tasks");
			return null;
		}
		
		String s = "At DatanodeDescriptor.java, in the func: getEncodingCommand";
		Debug.writeTime();
		Debug.writeDebug(s);
		
		DatanodeDescriptor[] sources = p.getSources();
		DatanodeDescriptor[][] targets = p.getTargets();
		RSGroup group = p.getGroup();
		Block[] blks = p.getBlocks();
		
		int rep = targets.length / blks.length; // The replication num
		
		Debug.writeDebug("We get a new encoding command:");
		Debug.writeDebug("The pre-encoding group is:" + group + ";");
		Debug.writeDebug("The Blocks to be encoded is:");
		for(int i = 0; i < blks.length; i++)
		{
			Debug.writeDebug(blks[i] + ";");
		}
		Debug.writeDebug("The sources come from:");
		for(int i = 0; i < sources.length; i++)
		{
			Debug.writeDebug("Source " + i + ": " + sources[i]);
		}
		Debug.writeDebug("The targets go to:");
		for(int i = 0; i < blks.length; i++)
		{
			Debug.writeDebug("The targets for " + blks[i] + " is:");
			for(int j = 0; j < rep; j++)
			{
				//Debug.writeDebug(targets[j + i * rep] + ";");
				Debug.writeDebug(targets[i][j] + ";");
			}
		}
		BlockCommand cmd = new BlockCommand(DatanodeProtocol.DNA_ENCODING, p);
		return cmd == null ? null : cmd;
	}
	
	BlockCommand getDecodingCommand() {	
		BlockSrcTargetPair p = decodingBlocks.poll();	
		if(p == null)
		{
			//Debug.writeDebug("Half return because there are no decoding tasks");
			return null;
		}

		String s = "At DatanodeDescriptor.java, in the func: getDecodingCommand.";
		Debug.writeTime();
		Debug.writeDebug(s);
		
		DatanodeDescriptor[] sources = p.getSources();
		DatanodeDescriptor[][] targets = p.getTargets();
		RSGroup group = p.getGroup();
		Block[] blks = p.getBlocks();
		Block[] grpBlks = group.getBlocks();

		Debug.writeDebug("We get a new decoding command!");	
		Debug.writeDebug("The pre-decoding group is:" + group + ";");
		Debug.writeDebug("The Blocks to be decoded to recover is:" + blks[0] + ";");

		Debug.writeDebug("The sources come from:");
		for(int i = 0; i < sources.length; i++)
		{
			if (sources[i] != null){
				if (sources[i].getName() != "NullForCode")
					Debug.writeDebug("Source " + i + " to get blocks:"
							+ grpBlks[i] + ": " + sources[i]);
				else
					break;
			}
		}

		Debug.writeDebug("The targets for recovered block " + blks[0] + " is:");
		for(int i = 0; i < targets[0].length; i++)
		{
				Debug.writeDebug(targets[0][i] + ";");
		}
		
		BlockCommand cmd = new BlockCommand(DatanodeProtocol.DNA_DECODING, p);
		return cmd == null ? null : cmd;
	}
	//TODO

	static private Block[] getBlockArray(Collection<Block> blocks, int max) {
		Block[] blockarray = null;
		synchronized (blocks) {
			int available = blocks.size();
			int n = available;
			if (max > 0 && n > 0) {
				if (max < n) {
					n = max;
				}
				// allocate the properly sized block array ...
				blockarray = new Block[n];

				// iterate tree collecting n blocks...
				Iterator<Block> e = blocks.iterator();
				int blockCount = 0;

				while (blockCount < n && e.hasNext()) {
					// insert into array ...
					blockarray[blockCount++] = e.next();

					// remove from tree via iterator, if we are removing
					// less than total available blocks
					if (n < available) {
						e.remove();
					}
				}
				assert (blockarray.length == n);

				// now if the number of blocks removed equals available blocks,
				// them remove all blocks in one fell swoop via clear
				if (n == available) {
					blocks.clear();
				}
			}
		}
		return blockarray;
	}

	void reportDiff(BlocksMap blocksMap, BlockListAsLongs newReport,
			Collection<Block> toAdd, Collection<Block> toRemove,
			Collection<Block> toInvalidate) {
		// place a deilimiter in the list which separates blocks
		// that have been reported from those that have not
		BlockInfo delimiter = new BlockInfo(new Block(), 1);
		boolean added = this.addBlock(delimiter);
		assert added : "Delimiting block cannot be present in the node";
		if (newReport == null)
			newReport = new BlockListAsLongs(new long[0]);
		// scan the report and collect newly reported blocks
		// Note we are taking special precaution to limit tmp blocks allocated
		// as part this block report - which why block list is stored as longs
		Block iblk = new Block(); // a fixed new'ed block to be reused with
									// index i
		for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
			iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i),
					newReport.getBlockGenStamp(i));
			BlockInfo storedBlock = blocksMap.getStoredBlock(iblk);
			if (storedBlock == null) {
				// If block is not in blocksMap it does not belong to any file
				toInvalidate.add(new Block(iblk));
				continue;
			}
			if (storedBlock.findDatanode(this) < 0) {// Known block, but not on
														// the DN
				// if the size differs from what is in the blockmap, then return
				// the new block. addStoredBlock will then pick up the right
				// size of this
				// block and will update the block object in the BlocksMap
				if (storedBlock.getNumBytes() != iblk.getNumBytes()) {
					toAdd.add(new Block(iblk));
				} else {
					toAdd.add(storedBlock);
				}
				continue;
			}
			// move block to the head of the list
			this.moveBlockToHead(storedBlock);
		}
		// collect blocks that have not been reported
		// all of them are next to the delimiter
		Iterator<Block> it = new BlockIterator(delimiter.getNext(0), this);
		while (it.hasNext())
			toRemove.add(it.next());
		this.removeBlock(delimiter);
	}

	/** Serialization for FSEditLog */
	void readFieldsFromFSEditLog(DataInput in) throws IOException {
		this.name = UTF8.readString(in);
		this.storageID = UTF8.readString(in);
		this.infoPort = in.readShort() & 0x0000ffff;

		this.capacity = in.readLong();
		this.dfsUsed = in.readLong();
		this.remaining = in.readLong();
		this.lastUpdate = in.readLong();
		this.xceiverCount = in.readInt();
		this.location = Text.readString(in);
		this.hostName = Text.readString(in);
		setAdminState(WritableUtils.readEnum(in, AdminStates.class));
	}

	/**
	 * @return Approximate number of blocks currently scheduled to be written to
	 *         this datanode.
	 */
	public int getBlocksScheduled() {
		return currApproxBlocksScheduled + prevApproxBlocksScheduled;
	}

	/**
	 * Increments counter for number of blocks scheduled.
	 */
	void incBlocksScheduled() {
		currApproxBlocksScheduled++;
	}

	/**
	 * Decrements counter for number of blocks scheduled.
	 */
	void decBlocksScheduled() {
		if (prevApproxBlocksScheduled > 0) {
			prevApproxBlocksScheduled--;
		} else if (currApproxBlocksScheduled > 0) {
			currApproxBlocksScheduled--;
		}
		// its ok if both counters are zero.
	}

	/**
	 * Adjusts curr and prev number of blocks scheduled every few minutes.
	 */
	private void rollBlocksScheduled(long now) {
		if ((now - lastBlocksScheduledRollTime) > BLOCKS_SCHEDULED_ROLL_INTERVAL) {
			prevApproxBlocksScheduled = currApproxBlocksScheduled;
			currApproxBlocksScheduled = 0;
			lastBlocksScheduledRollTime = now;
		}
	}
}
