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

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants;
import java.io.*;
import java.security.*;

/**
 * @author robeen
 * @version 1.0
 * @date 1010/3/18
 */

class Coder {
	RSCoder m_rs;
	int buffer_size = FSConstants.BUFFER_SIZE; 
	//int tail_size = 36;
	byte[][] buf;
	//Configuration conf = new Configuration();
	//long blockSize = conf.getLong("dfs.block.size",
			//FSConstants.DEFAULT_BLOCK_SIZE);
	public Coder() {
		m_rs = new RSCoder();
		RSCoder.setup_tables();
		RSCoder.CalculateValue();	
	}
	
	public byte[] getBuffer(int idx) {
		assert(idx >= 0 && idx < buf.length);
		return buf[idx];
		
	}
	
	//public byte[] CreateFileDigest(String fileName) throws IOException {
		//return FileMD5.getFileMD5String(fileName);
	//}
	
	/**
	 * <p> Encode the input data stream </br>
	 * fsIn refers to the pre-encode stream, which sums up to tCut; </br>
	 * fsOut refers to the encoded stream, which sums up to tCut + tMore. 
	 * @param fsIn The input data stream
	 * @param fsOut The output encoded data stream
	 * @param tCut The original pre-encoding blocks;
	 * @param tMore The redundant blocks after encode;
	 * @return void
	 * @throws IOException 
 	 */
	public void FileStreamEncode(DataInputStream[] fsIn,
			/*DataOutputStream[] fsOut,*/ short tCut, short tMore) throws IOException {
		// Just for test, we set the tCut and tMore here
		//short tCut = (short) fsIn.length;
		//short tMore = (short) fsOut.length;
		if(fsIn.length != tCut)
		{
			// TODO Set up an error log: Get a bad input stream
			return;
		}
		buf = new byte[tMore][buffer_size];
		byte[][] buffers = new byte[tCut][buffer_size];
		byte[] temp;	
		short[][] InputBytes = new short[tCut + tMore][];		
		m_rs.InitialCauchyMatrix(tCut, tMore);
		int count = 1;
		ByteArrayOutputStream[] bufferOut = new ByteArrayOutputStream[tMore];
		for(int i = 0; i < tMore; i++)
		{
			bufferOut[i] = new ByteArrayOutputStream();
		}
		while (true) {			
			for (int i = 0; i < tCut && count > 0; i++)		
			{
				count = fsIn[i].read(buffers[i], 0, buffer_size);
			}
			if (count <= 0)
				break;
			else {
				for (int i = 0; i < tCut + tMore; i++) {
					InputBytes[i] = new short[count];
					if (i >= 0 && i <= tCut - 1) {
						for (int k = 0; k < count; k++)
							if (buffers[i][k] < 0)
								InputBytes[i][k] = (short) (buffers[i][k] + 256);
							else
								InputBytes[i][k] = (short) buffers[i][k];
					}
				}
			}
			m_rs.RSEncode(InputBytes, tMore);
			temp = new byte[count];
			for (int i = 0; i < tMore; i++) {				
				for (int k = 0; k < count; k++)
					temp[k] = (byte) InputBytes[i+tCut][k];
				//fsOut[i].write(temp, 0, count); // For test
				bufferOut[i].write(temp, 0, count);
			}
			
			if(count < buffer_size)
				break;

		}
		
		for (int i = 0; i < tCut; i++) {
			fsIn[i].close();
		}
		for (int i = 0; i < tMore; i++) {
			//fsOut[i].close();
			buf[i] = bufferOut[i].toByteArray();
			bufferOut[i].close();
		}
	}
	
	/**
	 * <p> For test the FileStreamEncode using the file.
	 * @param sFile The input filename for test;
	 * @param tType useless parameters;
	 * @param tCut The original blocks of sFile;
	 * @param tMore The redundant blocks after erasure encode;
	 * @return true Successfully encoded false not for now
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException
 	 */
	public boolean Encode(String sFile, short tType, short tCut, short tMore) throws NoSuchAlgorithmException, IOException {
		DataInputStream[] fsIn = new DataInputStream[tCut];
		DataOutputStream[] fsOut = new DataOutputStream[tMore];
		for (int i = 0; i < tCut; i++) {
			fsIn[i] = new DataInputStream(new BufferedInputStream(
					new FileInputStream(sFile + ".RS_" + i)));
		}
		for (int i = 0; i < tMore; i++) {
			int j = i + tCut;
			fsOut[i] = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(sFile + ".RS_" + j)));
		}

		//FileStreamEncode(fsIn, fsOut, tCut, tMore);
		return true;
	}
		
	/**
	 * <p> Decode the specifed block with the input stream.
	 * @param fsIn Input data stream for decode;
	 * @param fsOut The output data stream which to be specified;
	 * @param tCut The original blocks of sFile;
	 * @param tMore The redundant blocks after erasure encode;
	 * @param index The index of the broken blocks in the RS Group;
	 * @return void
	 * @throws IOException 
 	 */ 
	public void FileStreamDecode(DataInputStream[] fsIn,
			/*DataOutputStream fsOut,*/ short tCut, short tMore, int index) throws IOException
	{	
		//short tMore = (short) (fsIn.length - tCut);
		if(fsIn.length != tCut + tMore){
			//TODO Set up an error log: Get a bad Inputstream
			return;
		}		
		if(!(index >= 0&& index < tCut + tMore)){
			//TODO Set up an error log: Get a bad index-reference to the broken block
			return;
		}
		short[] NotNull = new short[tCut];	
		buf = new byte[1][buffer_size];
		byte[][] buffers = new byte[tCut][buffer_size];
		ByteArrayOutputStream bufferOut = new ByteArrayOutputStream();
		int count = 0;
		for (short i = 0; i < fsIn.length&&count<tCut; i++)
			if (fsIn[i] != null) {
				NotNull[count] = i;		
				count++;
			}
        if(count < tCut) {
        	//System.out.println("Recovery fail...");
        	// TODO Set up an system log: 
        	// Current recovery has broken down because failures overwhelmed
        	return;
        }
		count = 1;
		m_rs.InitialInvertedCauchyMatrix(tCut, tMore, NotNull);
		short[][] InputBytes = new short[tCut][];
		while (true) {
			for (int i = 0; i < tCut && count > 0; i++) {
				int j = NotNull[i];				
				count = fsIn[j].read(buffers[i], 0, buffer_size);							
			}
			if (count <= 0)
				break;
			else {
				for (int i = 0; i < tCut; i++) {
					InputBytes[i] = new short[count];
					for (int k = 0; k < count; k++)
						if (buffers[i][k] < 0)
							InputBytes[i][k] = (short) (buffers[i][k] + 256);
						else
							InputBytes[i][k] = (short) buffers[i][k];
					buffers[i] = new byte[count];
				}
			}
			m_rs.RSDecode(InputBytes, tCut, tMore,index);           
			for (int k = 0; k < count; k++)
				buffers[index][k] = (byte)InputBytes[index][k];
			//fsOut.write(buffers[index],0,count);
			bufferOut.write(buffers[index], 0, count);
			if(count < buffer_size)
				break;
		}
		for (int i = 0; i < fsIn.length; i++)
			if(fsIn[i]!=null)
			  fsIn[i].close();
		//fsOut.close();
		buf[0] = bufferOut.toByteArray();
		bufferOut.close();
	}

	/**
	 * <p> For test the FileStreamDecode using the file.
	 * @param Dir The directory of the pre-decoded file;
	 * @param sFile The fileName of the output decoded block;
	 * @param tCut The original blocks of sFile;
	 * @param tMore The redundant blocks after erasure encode;
	 * @param index The specified 
	 * @return true Successfully decoded false not for now
	 * @throws IOException 
	 * @throws NumberFormatException
 	 */
	public boolean Decode(String Dir,String sFile,int tCut,int tMore,int index) throws NumberFormatException, IOException 
	{
		File dir = new File(Dir);
		File ls[] = dir.listFiles();
		DataInputStream[] fsIn = new DataInputStream[tCut+tMore];
		for(int i=0;i<tCut+tMore;i++)
			fsIn[i]=null;
		for (int k = 0; k < ls.length; k++)
			if (ls[k].isFile()) 
			{								
				String[] s = ls[k].getAbsolutePath().split("_");
				if(Integer.parseInt(s[s.length - 1])==index)
				{
					for(int i=0;i<tCut+tMore;i++)
						if(fsIn[i]!=null)
							fsIn[i].close();												
					return true;
				}
				fsIn[Integer.parseInt(s[s.length - 1])]=new DataInputStream(new BufferedInputStream(
						new FileInputStream(ls[k].getAbsolutePath())));				
			}		
		
		//DataOutputStream fsOut = new DataOutputStream(new BufferedOutputStream(
				//new FileOutputStream(sFile + ".RS_" + index)));;
		//FileStreamDecode(fsIn, fsOut,(short)tCut, (short)tMore, index);
		return true;
	}	
}