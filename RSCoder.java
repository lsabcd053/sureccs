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

class RSCoder
{	
    static int NW = (1 << 8);
    static short[] gflog;
    static short[] gfilog;
    short[][] maxtrix;
    short[][] Inverted;
    short[][] E;
    short[][] A;
    static short[][] div;
    static short[][] mult;
    
    static public void setup_tables()
    {        
        gflog = new short[NW];
        gfilog = new short[NW];
        int b = 1;
        for (int log = 0; log < NW - 1; log++)
        {
            gflog[b] = (short)log;
            gfilog[log] = (short)b;
            b = (b << 1);
            if ((b & (0x0100)) != 0) b = (b ^ (0x011D));
        }
        
    }
    
  //GF multiply
    static short multV(int a, int b)
    {
        int sum_log;
        if (a == 0 || b == 0) return 0;
        sum_log = gflog[a] + gflog[b];
        if (sum_log >= (NW - 1)) sum_log -= (NW - 1);
        return gfilog[sum_log];
    }
    
  //GF divide
    static short divV(int a, int b)
    {
        int diff_log;
        if (a == 0) return 0;
        if (b == 0) return 0;
        diff_log = gflog[a] - gflog[b];
        if (diff_log < 0) diff_log += NW - 1;
        return gfilog[diff_log];
    }
    
    static public void CalculateValue()
    {
        mult=new short[NW][NW];
        div=new short[NW][NW];
        /*for (int i = 0; i < NW; i++)
        { 
           mult[i]=new short[NW];
           div[i]=new short[NW];
        }*/
        for(int i=0;i<NW;i++)
            for (int j = 0; j < NW; j++)
            {     
				mult[i][j] = multV(i,j);
                div[i][j] = divV(i,j);
            }
    }

    public void InitialCauchyMatrix(short cut, short redundance)
    {            
        E = new short[cut][];
        Inverted = new short[cut][];
        maxtrix = new short[redundance][];
        for (int i = 0; i < cut; i++)
        {
            E[i] = new short[cut];
            Inverted[i] = new short[cut];
            for (int j = 0; j < cut; j++)
                if (i == j) E[i][j] = 1;
                else E[i][j] = 0;
        }
        for (short i = 0; i < redundance; i++)
            maxtrix[i] = new short[cut];
        for (short j = 0; j < redundance; j++)
            for (short i = 0; i < cut; i++)
                maxtrix[j][i] = div[1][(j) ^ (i + redundance)];
    }    
  
    //swap  
    void swap(int j, short cut)
    {
        short max = Inverted[j][j];
        int i = -1;
        for (int k = j + 1; k < cut; k++)
        {
            if (Inverted[k][j] > max)
            {
                i = k;
                max = Inverted[k][j];
            }
        }
        if (i != -1)
        {
            short[] temp;
            temp = E[j];
            E[j] = E[i];
            E[i] = temp;
            temp = Inverted[j];
            Inverted[j] = Inverted[i];
            Inverted[i] = temp;
        }
    }
    
    public void InitialInvertedCauchyMatrix(short cut, short redundance, short[] ParaNotNull/*, int len*/)
    {
        InitialCauchyMatrix(cut, redundance);        
        for (short i = 0; i < cut; i++)
        {            
            int j = ParaNotNull[i];
            if (j < cut)
            	System.arraycopy(E[j], 0, Inverted[i], 0, E[j].length);
            else            	
            	System.arraycopy(maxtrix[j-cut], 0, Inverted[i], 0, maxtrix[j-cut].length);            	 
        }
        for (int i = 0; i < cut; i++)
        {
            swap(i, cut);
            int k = Inverted[i][i];
            if (k > 1)
            {
                for (int j = 0; j < cut; j++)
                {
                    Inverted[i][j] = div[Inverted[i][j]][k];
                    E[i][j] = div[E[i][j]][k];
                }
            }
            for (int j = 0; j < (cut); j++)
            {
                if ((j == i) || (Inverted[j][i] == 0)) continue;
                k = Inverted[j][i];
                for (int t = 0; t < cut; t++)
                {
                    Inverted[j][t] = div[Inverted[j][t]][ k];
                    Inverted[j][t] ^= Inverted[i][t];
                    E[j][t] = div[E[j][t]][k];
                    E[j][t] ^= E[i][t];
                }
            }
        }
        for (int i = 0; i < cut; i++)
        {
            if ((Inverted[i][i] != 1))
                for (int j = 0; j < cut; j++)
                    E[i][j] = div[E[i][j]][Inverted[i][i]];
        }

    }
    
    public void RSEncode(short[][] buffers, short tMore)
    {
        short cut = (short)(buffers.length - tMore);
        int len = buffers[0].length;
        for (int k = cut; k < cut + tMore; k++)
        {
            for (int i = 0; i < len; i++)
            {
                buffers[k][i] = 0;
                for (int j = 0; j < cut; j++)
                    buffers[k][i] ^= mult[maxtrix[k - cut][j]][buffers[j][i]];
            }
        }

    }
    //public void RSDecode(short[][] buffers, short cut, short redundance)
    //{    	
    	//int len=buffers[0].length;
    	//A=new short[cut][];
        //for (int i = 0; i < cut; i++)
        //{
        	//A[i]=new short[buffers[i].length];
        	//System.arraycopy(buffers[i], 0, A[i], 0, buffers[i].length);
            //buffers[i] = new short[len];
        //}
        //for (int i = 0; i < cut; i++)
            //for (int j = 0; j < cut; j++)
                //for (int w = 0; w < len; w++)
                   // buffers[i][w] ^=mult[E[i][j]][A[j][w]];
    //}
    public void RSDecode(short[][] buffers, short cut, short redundance,int index)
    {
    	A=new short[cut][];
    	int len=buffers[0].length;
    	//int i=index;
        for (int i = 0; i < cut; i++)
        {
        	A[i]=new short[buffers[i].length];
        	System.arraycopy(buffers[i], 0, A[i], 0, buffers[i].length);
            buffers[i] = new short[len];
        }
        int i=index;
        //for (int i = 0; i < cut; i++)
            for (int j = 0; j < cut; j++)
                for (int w = 0; w < len; w++)
                    buffers[i][w] ^=mult[E[i][j]][A[j][w]];
    }

}
    