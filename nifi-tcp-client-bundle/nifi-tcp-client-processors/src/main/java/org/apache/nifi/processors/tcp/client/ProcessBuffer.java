package org.apache.nifi.processors.tcp.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessBuffer {

	private Byte dataLinkEscapeByte;
	private Byte startOfMessageByte;
	private Byte endOfMessageByte;
	private int maxReceiveBufferSize;
	private int maxSendBufferSize;

	private final ByteArrayOutputStream readCurrBytes;
	private final ByteArrayOutputStream writeCurrBytes;

	private boolean lastDataLinkEscapeByte;
	private boolean lastStartOfMessageByte;
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public ProcessBuffer(final Byte dataLinkEscapeByte, final Byte startOfMessageByte, final Byte endOfMessageByte,
			final int maxReceiveBufferSize, final int maxSendBufferSize) {
		this.dataLinkEscapeByte = dataLinkEscapeByte;
		this.startOfMessageByte = startOfMessageByte;
		this.endOfMessageByte = endOfMessageByte;
		this.maxReceiveBufferSize = maxReceiveBufferSize;
		this.maxSendBufferSize = maxSendBufferSize;
		this.readCurrBytes = new ByteArrayOutputStream(maxReceiveBufferSize);
		this.writeCurrBytes = new ByteArrayOutputStream(maxSendBufferSize);
		this.lastDataLinkEscapeByte = false;
		this.lastStartOfMessageByte = false;

	}

	// return list of packets destuffed
	public List<byte[]> deStuffPacket(final int bytesRead, final byte[] buffer) {

		List<byte[]> destuffPackets = new ArrayList<byte[]>();

		// go through the buffer looking for packages
		for (int i = 0; i < bytesRead; i++) {
			final byte currByte = buffer[i];
			int lengthOfCurrBytes = readCurrBytes.size();

			if (lengthOfCurrBytes >= maxReceiveBufferSize) {
				logger.warn("Packet excess buffer size");
				clearReadCurrBytes();
			}

			// locking for a packet
			byte[] destuffPacket;
			if (this.startOfMessageByte != null && this.endOfMessageByte != null && this.dataLinkEscapeByte != null) {
				destuffPacket = deStuffPacketStartAndEndWithEscape(currByte);
			} else if (this.startOfMessageByte != null && this.endOfMessageByte != null) {
				destuffPacket = deStuffPacketStartAndEndWithoutEscape(currByte);
			} else if (this.endOfMessageByte != null && this.dataLinkEscapeByte != null) {
				destuffPacket = deStuffPacketEndWithEscape(currByte);
			} else if (this.endOfMessageByte != null) {
				destuffPacket = deStuffPacketEndWithoutEscape(currByte);
			} else {
				destuffPacket = null;
			}
			if (destuffPacket != null) {
				destuffPackets.add(destuffPacket);
			}
		}

		return destuffPackets;

	}

	// Example of package: DLE STX A ... B ... DLE DLE ... C ... D DLE ETX
	// Example of package deStuffed: A ... B ... DLE ... C ... D
	private byte[] deStuffPacketStartAndEndWithEscape(byte currByte) {
		int lengthOfCurrBytes = readCurrBytes.size();

		if (lengthOfCurrBytes == 0) {
			if (lastDataLinkEscapeByte) {
				if (currByte == this.startOfMessageByte) {
					lastStartOfMessageByte = true;
				} else {
					// not package init
					clearReadCurrBytes();
				}
				lastDataLinkEscapeByte = false;
				return null;
			} else if (lastStartOfMessageByte) {
				lastStartOfMessageByte = false;
				// not return
			} else {
				if (currByte == this.dataLinkEscapeByte) {
					lastDataLinkEscapeByte = true;
				} else {
					// not package init
					clearReadCurrBytes();
				}
				return null;
			}
		}

		if (currByte == this.dataLinkEscapeByte && !this.lastDataLinkEscapeByte) {
			lastDataLinkEscapeByte = true;
			return null;
		} else if (currByte == this.endOfMessageByte && this.lastDataLinkEscapeByte) {
			// packet complete destuffed
			byte[] packageByteArray = readCurrBytes.toByteArray();
			clearReadCurrBytes();
			return packageByteArray;
		} else if (this.lastDataLinkEscapeByte) {
			lastDataLinkEscapeByte = false;
		}

		readCurrBytes.write(currByte);
		return null;
	}

	// Example of package: STX A ... B ... C ... D ETX
	// Example of package deStuffed: A ... B ... C ... D
	private byte[] deStuffPacketStartAndEndWithoutEscape(byte currByte) {
		int lengthOfCurrBytes = readCurrBytes.size();

		if (lengthOfCurrBytes == 0) {
			if (currByte == this.startOfMessageByte) {
				lastStartOfMessageByte = true;
				return null;
			} else if (lastStartOfMessageByte) {
				lastStartOfMessageByte = false;
				// not return
			} else {
				// not package init
				clearReadCurrBytes();
			}
		}

		if (currByte == this.endOfMessageByte) {
			// packet complete destuffed
			byte[] packageByteArray = readCurrBytes.toByteArray();
			clearReadCurrBytes();
			return packageByteArray;
		}

		readCurrBytes.write(currByte);
		return null;
	}

	// Example of package: A ... B ... DLE DLE ... C ... D DLE ETX
	// Example of package deStuffed: A ... B ... DLE ... C ... D
	private byte[] deStuffPacketEndWithEscape(byte currByte) {
		
		if (currByte == this.dataLinkEscapeByte && !this.lastDataLinkEscapeByte) {
			lastDataLinkEscapeByte = true;
			return null;
		} else if (currByte == this.endOfMessageByte && this.lastDataLinkEscapeByte) {
			// packet complete destuffed
			byte[] packageByteArray = readCurrBytes.toByteArray();
			clearReadCurrBytes();
			return packageByteArray;
		} else if (this.lastDataLinkEscapeByte) {
			lastDataLinkEscapeByte = false;
		}

		readCurrBytes.write(currByte);
		return null;
	}

	// Example of package: A ... B ... C ... D ETX
	// Example of package deStuffed: A ... B ... C ... D
	private byte[] deStuffPacketEndWithoutEscape(byte currByte) {

		if (currByte == this.endOfMessageByte) {
			// packet complete destuffed
			byte[] packageByteArray = readCurrBytes.toByteArray();
			clearReadCurrBytes();
			return packageByteArray;
		}

		readCurrBytes.write(currByte);
		return null;
	}
	
	
	// return message suffed
	public byte[] stuffPacket(final byte[] buffer) {

		byte[] stuffPacket;
		int bytesWrite = buffer.length;
		boolean isDataLinkEscape;
		byte[] startMessage;
		byte[] endMessage;
		
		if (bytesWrite >= maxSendBufferSize) {
			logger.warn("Packet excess buffer size");
			clearWriteCurrBytes();
			return null;
		}

		if (this.startOfMessageByte != null && this.endOfMessageByte != null && this.dataLinkEscapeByte != null) {
			
			// Example of package: A ... B ... DLE ... C ... D
			// Example of package stuffed: DLE STX A ... B ... DLE DLE ... C ... D DLE ETX
			
			startMessage = new byte[]{this.dataLinkEscapeByte, this.startOfMessageByte};
			endMessage = new byte[]{this.dataLinkEscapeByte, this.endOfMessageByte};
			isDataLinkEscape = true;
		} else if (this.startOfMessageByte != null && this.endOfMessageByte != null) {
			
			// Example of package: A ... B ... C ... D
			// Example of package stuffed: STX A ... B ... C ... D ETX
			
			startMessage = new byte[]{this.startOfMessageByte};
			endMessage = new byte[]{this.endOfMessageByte};
			isDataLinkEscape = false;
		} else if (this.endOfMessageByte != null && this.dataLinkEscapeByte != null) {
			
			// Example of package: A ... B ... DLE ... C ... D
			// Example of package stuffed: A ... B ... DLE DLE ... C ... D DLE ETX
			
			startMessage = new byte[0];
			endMessage = new byte[]{this.dataLinkEscapeByte, this.endOfMessageByte};
			isDataLinkEscape = true;
		} else if (this.endOfMessageByte != null) {
			
			// Example of package: A ... B ... C ... D
			// Example of package stuffed: A ... B ... C ... D ETX
			
			startMessage = new byte[0];
			endMessage = new byte[]{this.endOfMessageByte};
			isDataLinkEscape = false;
		} else {
			return null;
		}

		// add start of message
		try {
			writeCurrBytes.write(startMessage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// go through the buffer looking dataLinkEscapeByte
		if (isDataLinkEscape) {
			for (int i = 0; i < bytesWrite; i++) {
				final byte currByte = buffer[i];
				
				if (currByte == this.endOfMessageByte) {
					this.writeCurrBytes.write(this.dataLinkEscapeByte);
				}
				this.writeCurrBytes.write(currByte);
			}
		}
		
		try {
			writeCurrBytes.write(endMessage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		stuffPacket = this.writeCurrBytes.toByteArray();
		clearWriteCurrBytes();
		return stuffPacket;

	}

	private void clearReadCurrBytes() {
		readCurrBytes.reset();
		lastDataLinkEscapeByte = false;
		lastStartOfMessageByte = false;
	}
	
	private void clearWriteCurrBytes() {
		writeCurrBytes.reset();
	}
	
	
	
	

}
