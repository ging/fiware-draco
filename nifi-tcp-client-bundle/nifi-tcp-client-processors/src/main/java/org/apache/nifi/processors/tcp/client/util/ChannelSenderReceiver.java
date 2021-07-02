/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.tcp.client.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Base class for sending messages over a channel.
 */
public abstract class ChannelSenderReceiver {

	protected final int port;
	protected final String host;
	protected final int maxReceiveBufferSize;
	protected final int maxSendBufferSize;
	

	protected volatile int timeout = 100 * 1000;
	protected volatile int timeoutConnect = 10 * 1000;
	protected volatile long lastUsed;

	public ChannelSenderReceiver(final String host, final int port, final int maxReceiveBufferSize,
			final int maxSendBufferSize) {
		this.port = port;
		this.host = host;
		this.maxReceiveBufferSize = maxReceiveBufferSize;
		this.maxSendBufferSize = maxSendBufferSize;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getTimeout() {
		return timeout;
	}

	/**
	 * @return the last time channel was used
	 */
	public long getLastUsed() {
		return lastUsed;
	}

	public void updateLastUsed() {
		this.lastUsed = System.currentTimeMillis();
	}

	/**
	 * Connects to the destination.
	 *
	 * @throws IOException if an error occurred opening the connection.
	 */
	public void connect() throws IOException {
		open();
		updateLastUsed();
	};

	/**
	 * Opens the connection to the destination.
	 *
	 * @throws IOException if an error occurred opening the connection.
	 */
	public abstract void open() throws IOException;

	/**
	 * Sends the given string over the channel.
	 *
	 * @param message the message to send over the channel
	 * @throws IOException if there was an error communicating over the channel
	 */
	public void send(final String message, final Charset charset) throws IOException {
		final byte[] bytes = message.getBytes(charset);
		send(bytes);
	}

	/**
	 * Sends the given data over the channel.
	 *
	 * @param data the data to send over the channel
	 * @throws IOException if there was an error communicating over the channel
	 */
	public void send(final byte[] data) throws IOException {
		write(data);
		updateLastUsed();

	}

	/**
	 * Receives the given data over the channel.
	 *
	 * @param data the data to receive over the channel
	 * @throws IOException if there was an error communicating over the channel
	 */
	public int receive(ByteBuffer byteBuffer, byte[] data) throws IOException {
		int bytesRead = read(byteBuffer, data);
		updateLastUsed();
		return bytesRead;

	}

	/**
	 * Read the given buffer to the underlying channel.
	 */
	public abstract int read(ByteBuffer byteBuffer, byte[] data) throws IOException;

	/**
	 * Write the given buffer to the underlying channel.
	 */
	public abstract void write(byte[] data) throws IOException;

	/**
	 * @return true if the underlying channel is connected
	 */
	public abstract boolean isConnected();

	/**
	 * Close the underlying channel
	 */
	public abstract void close();

}
