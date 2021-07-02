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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.remote.io.socket.SocketChannelOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends and reads messages over a SocketChannel.
 */
public class SocketChannelSenderReceiver extends ChannelSenderReceiver {

	protected SocketChannel socketChannel;
	protected SocketChannelOutputStream socketChannelOutput;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public SocketChannelSenderReceiver(final String host, final int port, final int maxReceiveBufferSize,
			final int maxSendBufferSize) {
		super(host, port, maxReceiveBufferSize, maxSendBufferSize);
	}

	@Override
	public void open() throws IOException {
		try {
			if (socketChannel == null || !socketChannel.isOpen()) {
				socketChannel = SocketChannel.open();
				socketChannel.configureBlocking(false);

				if (maxReceiveBufferSize > 0) {
					socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, maxReceiveBufferSize);
					final int actualReceiveBufSize = socketChannel.getOption(StandardSocketOptions.SO_RCVBUF);
					if (actualReceiveBufSize < maxReceiveBufferSize) {
						logger.warn("Attempted to set Socket Receive Buffer Size to " + maxReceiveBufferSize
								+ " bytes but could only set to " + actualReceiveBufSize + "bytes. You may want to "
								+ "consider changing the Operating System's maximum send buffer");
					}
				}

				if (maxSendBufferSize > 0) {
					socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, maxSendBufferSize);
					final int actualSendBufSize = socketChannel.getOption(StandardSocketOptions.SO_SNDBUF);
					if (actualSendBufSize < maxSendBufferSize) {
						logger.warn("Attempted to set Socket Send Buffer Size to " + maxSendBufferSize
								+ " bytes but could only set to " + actualSendBufSize + "bytes. You may want to "
								+ "consider changing the Operating System's maximum send buffer");
					}
				}

			}

			if (!socketChannel.isConnected()) {
				final long startTime = System.currentTimeMillis();
				final InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName(host), port);

				if (!socketChannel.connect(socketAddress)) {
					while (!socketChannel.finishConnect()) {
						if (System.currentTimeMillis() > startTime + timeoutConnect) {
							throw new SocketTimeoutException("Timed out connecting to " + host + ":" + port);
						}

						try {
							Thread.sleep(50L);
						} catch (final InterruptedException e) {
						}
					}
				}

				if (logger.isDebugEnabled()) {
					final SocketAddress localAddress = socketChannel.getLocalAddress();
					if (localAddress != null && localAddress instanceof InetSocketAddress) {
						final InetSocketAddress inetSocketAddress = (InetSocketAddress) localAddress;
						logger.debug("Connected to local port {}", new Object[] { inetSocketAddress.getPort() });
					}
				}

				socketChannelOutput = new SocketChannelOutputStream(socketChannel);
			}
		} catch (final IOException e) {
			IOUtils.closeQuietly(socketChannel);
			throw e;
		}
	}

	@Override
	public int read(ByteBuffer byteBuffer, byte[] byteBufferArray) throws IOException {
		int bytesRead = socketChannel.read(byteBuffer);
		byte[] data = byteBuffer.array();
		System.arraycopy(data, 0, byteBufferArray, 0, data.length);
		return bytesRead;
	}

	@Override
	public void write(byte[] data) throws IOException {
		socketChannelOutput.write(data);
	}

	@Override
	public boolean isConnected() {
		return socketChannel != null && socketChannel.isConnected();
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(socketChannelOutput);
		IOUtils.closeQuietly(socketChannel);
		socketChannelOutput = null;
		socketChannel = null;
	}

	public SocketChannel getChannel() {
		return this.socketChannel;
	}

}
