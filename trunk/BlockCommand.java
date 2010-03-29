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
import java.util.List;

import org.apache.hadoop.dfs.DatanodeDescriptor.BlockSrcTargetPair;
import org.apache.hadoop.dfs.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.io.*;

abstract class DatanodeCommand implements Writable {
	static class Register extends DatanodeCommand {
		private Register() {
			super(DatanodeProtocol.DNA_REGISTER);
		}

		public void readFields(DataInput in) {
		}

		public void write(DataOutput out) {
		}
	}

	static class BlockReport extends DatanodeCommand {
		private BlockReport() {
			super(DatanodeProtocol.DNA_BLOCKREPORT);
		}

		public void readFields(DataInput in) {
		}

		public void write(DataOutput out) {
		}
	}

	static class Finalize extends DatanodeCommand {
		private Finalize() {
			super(DatanodeProtocol.DNA_FINALIZE);
		}

		public void readFields(DataInput in) {
		}

		public void write(DataOutput out) {
		}
	}

	static { // register a ctor
		WritableFactories.setFactory(Register.class, new WritableFactory() {
			public Writable newInstance() {
				return new Register();
			}
		});
		WritableFactories.setFactory(BlockReport.class, new WritableFactory() {
			public Writable newInstance() {
				return new BlockReport();
			}
		});
		WritableFactories.setFactory(Finalize.class, new WritableFactory() {
			public Writable newInstance() {
				return new Finalize();
			}
		});
	}

	static final DatanodeCommand REGISTER = new Register();
	static final DatanodeCommand BLOCKREPORT = new BlockReport();
	static final DatanodeCommand FINALIZE = new Finalize();

	private int action;

	public DatanodeCommand() {
		this(DatanodeProtocol.DNA_UNKNOWN);
	}

	DatanodeCommand(int action) {
		this.action = action;
	}

	int getAction() {
		return this.action;
	}

	// /////////////////////////////////////////
	// Writable
	// /////////////////////////////////////////
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.action);
	}

	public void readFields(DataInput in) throws IOException {
		this.action = in.readInt();
	}
}

/****************************************************
 * A BlockCommand is an instruction to a datanode regarding some blocks under
 * its control. It tells the DataNode to either invalidate a set of indicated
 * blocks, or to copy a set of indicated blocks to another DataNode.
 * 
 ****************************************************/
class BlockCommand extends DatanodeCommand {
	Block blocks[];
	DatanodeInfo targets[][];
	
	// TODO add sources here
	DatanodeInfo sources[][];
	
	RSGroup group;
	int index;
	// TODO

	public BlockCommand() {
	}

	/**
	 * Create BlockCommand for transferring blocks to another datanode
	 * 
	 * @param blocks
	 *            blocks to be transferred
	 */
	BlockCommand(int action, List<BlockTargetPair> blocktargetlist) {
		super(action);

		blocks = new Block[blocktargetlist.size()];
		targets = new DatanodeInfo[blocks.length][];
		for (int i = 0; i < blocks.length; i++) {
			BlockTargetPair p = blocktargetlist.get(i);
			blocks[i] = p.block;
			targets[i] = p.targets;
		}
	}
	
	//TODO blockCommand for coding
	/**
	 * Create BlockCommand for coding blocks
	 * 
	 * @param blocks
	 *            blocks to be coded
	 */
	BlockCommand(int action, BlockSrcTargetPair p, int yes) {
		super(action);

		blocks = new Block[p.blocks.length];
		targets = new DatanodeInfo[1][];
		sources = new DatanodeInfo[1][];
		//targets = new DatanodeInfo[blocks.length][];
		blocks = p.blocks;
		targets[0] = p.targets;
		sources[0] = p.sources;
		group = p.group;
		index = p.index;
	}
	//TODO 

	private static final DatanodeInfo[][] EMPTY_TARGET = {};

	/**
	 * Create BlockCommand for the given action
	 * 
	 * @param blocks
	 *            blocks related to the action
	 */
	BlockCommand(int action, Block blocks[]) {
		super(action);
		this.blocks = blocks;
		this.targets = EMPTY_TARGET;
	}

	Block[] getBlocks() {
		return blocks;
	}

	DatanodeInfo[][] getTargets() {
		return targets;
	}
	
	// TODO add the method to get sources for coding
	DatanodeInfo[][] getSources(){
		return this.sources;
	}
	
	RSGroup getGroup(){
		return this.group;
	}
	
	int getIndex(){
		return this.index;
	}

	// /////////////////////////////////////////
	// Writable
	// /////////////////////////////////////////
	static { // register a ctor
		WritableFactories.setFactory(BlockCommand.class, new WritableFactory() {
			public Writable newInstance() {
				return new BlockCommand();
			}
		});
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(blocks.length);
		for (int i = 0; i < blocks.length; i++) {
			blocks[i].write(out);
		}
		// TODO sources seriable
		out.writeInt(sources.length);
		for (int i = 0; i < sources.length; i++) {
			out.writeInt(sources[i].length);
			for (int j = 0; j < sources[i].length; j++) {
				sources[i][j].write(out);
			}
		}
		// TODO
		out.writeInt(targets.length);
		for (int i = 0; i < targets.length; i++) {
			out.writeInt(targets[i].length);
			for (int j = 0; j < targets[i].length; j++) {
				targets[i][j].write(out);
			}
		}
		
		// TODO group seriable
		group.write(out);
		out.writeInt(index);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.blocks = new Block[in.readInt()];
		for (int i = 0; i < blocks.length; i++) {
			blocks[i] = new Block();
			blocks[i].readFields(in);
		}
		
		// TODO sources seriable
		this.sources = new DatanodeInfo[in.readInt()][];
		for (int i = 0; i < sources.length; i++) {
			this.sources[i] = new DatanodeInfo[in.readInt()];
			for (int j = 0; j < sources[i].length; j++) {
				sources[i][j] = new DatanodeInfo();
				sources[i][j].readFields(in);
			}
		}
		// TODO

		this.targets = new DatanodeInfo[in.readInt()][];
		for (int i = 0; i < targets.length; i++) {
			this.targets[i] = new DatanodeInfo[in.readInt()];
			for (int j = 0; j < targets[i].length; j++) {
				targets[i][j] = new DatanodeInfo();
				targets[i][j].readFields(in);
			}
		}
		
		// TODO group seriable
		group.readFields(in);
		index = in.readInt();
	}
}
