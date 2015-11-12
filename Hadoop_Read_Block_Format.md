# Introduction #

The interactivity protocol while client needs to read a block from the datanode.
This is comment from the hadoop source code in java version.


# Details #

> /*** Protocol when a client reads data from Datanode (Cur Ver: 9):
    * 
    * Client's Request : =================
    * 
    * Processed in DataXceiver:
    * +----------------------------------------------+ | Common Header | 1 byte
    * OP == OP\_READ\_BLOCK | +----------------------------------------------+
    * 
    * Processed in readBlock() :
    * +----------------------------------------------
    * ---------------------------+ | 8 byte Block ID | 8 byte genstamp | 8 byte
    * start offset | 8 byte length |
    * +------------------------------------------
    * -------------------------------+
    * 
    * Client sends optional response only at the end of receiving data.
    * 
    * DataNode Response : ===================
    * 
    * In readBlock() : If there is an error while initializing BlockSender :
    * +---------------------------+ | 2 byte OP\_STATUS\_ERROR | and connection
    * will be closed. +---------------------------+ Otherwise
    * +---------------------------+ | 2 byte OP\_STATUS\_SUCCESS |
    * +---------------------------+
    * 
    * Actual data, sent by BlockSender.sendBlock() :
    * 
    * ChecksumHeader : +--------------------------------------------------+ | 1
    * byte CHECKSUM\_TYPE | 4 byte BYTES\_PER\_CHECKSUM |
    * +--------------------------------------------------+ Followed by actual
    * data in the form of PACKETS: +------------------------------------+ |
    * Sequence of data PACKETs .... | +------------------------------------+
    * 
    * A "PACKET" is defined further below.
    * 
    * The client reads data until it receives a packet with "LastPacketInBlock"
    * set to true or with a zero length. If there is no checksum error, it
    * replies to DataNode with OP\_STATUS\_CHECKSUM\_OK:
    * 
    * Client optional response at the end of data transmission :
    * +------------------------------+ | 2 byte OP\_STATUS\_CHECKSUM\_OK |
    * +------------------------------+
    * 
    * PACKET : Contains a packet header, checksum and data. Amount of data
    * ======== carried is set by BUFFER\_SIZE.
    * 
    * +-----------------------------------------------------+ | 4 byte packet
    * length (excluding packet header) |
    * +-----------------------------------------------------+ | 8 byte offset
    * in the block | 8 byte sequence number |
    * +-----------------------------------------------------+ | 1 byte
    * isLastPacketInBlock |
    * +-----------------------------------------------------+ | 4 byte Length
    * of actual data | +-----------------------------------------------------+
    * | x byte checksum data. x is defined below |
    * +-----------------------------------------------------+ | actual data
    * ...... | +-----------------------------------------------------+
    * 
    * x = (length of data + BYTE\_PER\_CHECKSUM - 1) / BYTES\_PER\_CHECKSUM\*CHECKSUM\_SIZE
    * 
    * CHECKSUM\_SIZE depends on CHECKSUM\_TYPE (usually, 4 for CRC32)
    * 
    * The above packet format is used while writing data to DFS also. Not all
    * the fields might be used while reading.
    * 
    ***
    * 