package org.apache.nifi.processors.tcp.client;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;

import org.apache.nifi.components.AllowableValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nifi.processors.tcp.client.util.ChannelSenderReceiver;
import org.apache.nifi.processors.tcp.client.util.SSLSocketChannelSenderReceiver;
import org.apache.nifi.processors.tcp.client.util.SocketChannelSenderReceiver;

/**
 * Base class to implement async TCP Client
 *
 */

public class SendingReceivingClient {

	private final String host;

	private final int port;

	private final int readingSize;

	private final ProcessBuffer processBuffer;

	private final ByteBuffer readingBuffer;

	private final Runnable clientTask;

	private volatile ExecutorService clientTaskExecutor;

	private volatile Selector selector;

	private final AtomicBoolean isRunning;

	private volatile ChannelSenderReceiver channelSenderReceiver;

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final AllowableValue TCP_VALUE = new AllowableValue("TCP", "TCP");
	
	private volatile MessageHandler messageHandler;

	/**
	 * 
	 * This constructor configures the address to bind to, the size of the buffer to
	 * use for reading and writing, and the byte pattern to use for demarcating the
	 * end/start of a message. the processBuffer for managing information
	 * 
	 */
	public SendingReceivingClient(String host, int port, Byte DLE, Byte STX, Byte ETX, int readingSize,
			int maxReceiveBufferSize, int maxSendBufferSize, int timeout, SSLContext sslContext) {

		this.host = host;
		this.port = port;
		this.readingSize = readingSize;
		this.readingBuffer = ByteBuffer.allocate(readingSize);
		this.isRunning = new AtomicBoolean();
		this.clientTask = new ClientsTask();

		if (sslContext != null) {
			this.channelSenderReceiver = new SSLSocketChannelSenderReceiver(host, port, maxReceiveBufferSize,
					maxSendBufferSize, sslContext);
		} else {
			this.channelSenderReceiver = new SocketChannelSenderReceiver(host, port, maxReceiveBufferSize,
					maxSendBufferSize);
		}
		this.channelSenderReceiver.setTimeout(timeout);
		this.processBuffer = new ProcessBuffer(DLE, STX, ETX, maxReceiveBufferSize, maxSendBufferSize);
	}

	/**
	 * Start the selector and the client
	 * 
	 */
	public void start() {
		if (this.isRunning.compareAndSet(false, true)) {
			try {
				if (logger.isDebugEnabled()) {
					logger.debug("Starting listener for " + SendingReceivingClient.this.getClass().getSimpleName());
				}
				if (this.selector == null || !this.selector.isOpen()) {
					this.selector = Selector.open();
					connect();
					this.clientTaskExecutor = Executors.newSingleThreadExecutor();
					this.clientTaskExecutor.execute(this.clientTask);
					if (logger.isDebugEnabled()) {
						logger.debug("Started listener for " + SendingReceivingClient.this.getClass().getSimpleName());
					}
				}

			} catch (Exception e) {
				this.stop();
				throw new IllegalStateException("Failed to start " + this.getClass().getName(), e);
			}
		}
	}

	/**
	 * Stop the selector and the client
	 * 
	 */
	public void stop() {
		if (this.isRunning.compareAndSet(true, false)) {
			if (this.selector != null && this.selector.isOpen()) {
				try {
					Set<SelectionKey> selectionKeys = new HashSet<>(this.selector.keys());
					for (SelectionKey key : selectionKeys) {
						key.cancel();
						SendingReceivingClient sendingReceivingClient = (SendingReceivingClient) key.attachment();
						// closing channels
						sendingReceivingClient.getChannelSenderReceiver().close();
						logger.info(this.getClass().getSimpleName() + " is stopped listening on "
								+ sendingReceivingClient.getHost() + ":" + sendingReceivingClient.getPort());
					}
					try {
						this.selector.close();
						logger.info(this.getClass().getSimpleName() + " selector stopped");
					} catch (Exception e) {
						logger.warn("Failure while closing selector", e);
					}
				} finally {
					if (this.clientTaskExecutor != null) {
						this.clientTaskExecutor.shutdown();
					}
				}
			}
		}
	}

	/**
	 * Connect the client
	 */
	public void connect() {
		try {
			this.channelSenderReceiver.connect();
			;
			ChannelSenderReceiver channelSenderReceiver = this.getChannelSenderReceiver();
			SocketChannel socketChannel = ((SocketChannelSenderReceiver) channelSenderReceiver).getChannel();
			socketChannel.register(this.selector, SelectionKey.OP_READ, this);

			if (logger.isInfoEnabled()) {
				logger.info("Successfully bound to " + this.getHost() + ":" + this.getPort());
			}
		} catch (Exception e) {
			logger.error("Failed to start " + this.getClass().getName(), e);
			throw new IllegalStateException(e);
		}
	}

	public void read() throws IOException {
		int bytesRead;
		byte[] socketBufferArray = new byte[readingSize];
		// preparing for read
		readingBuffer.clear();
		// read until no more data
		try {
			while ((bytesRead = this.channelSenderReceiver.receive(readingBuffer, socketBufferArray)) > 0) {
				List<byte[]> packets = processBuffer.deStuffPacket(bytesRead, socketBufferArray);
				for (byte[] packet : packets) {
					messageHandler.handle(packet);
				}
			}
		} catch (SocketTimeoutException ste) {
			// SSLSocketChannel will throw this exception when 0 bytes are read and the
			// timeout threshold
			// is exceeded, we don't want to close the connection in this case
			bytesRead = 0;
		}

		if (bytesRead < 0) {
			channelSenderReceiver.close();
			return;
		}
	}

	public boolean isTimeout() {

		if (this.selector != null && this.selector.isOpen()) {
			Set<SelectionKey> selectionKeys = new HashSet<>(this.selector.keys());
			for (SelectionKey key : selectionKeys) {
				SendingReceivingClient sendingReceivingClient = (SendingReceivingClient) key.attachment();
				ChannelSenderReceiver channelSenderReceiver = sendingReceivingClient.getChannelSenderReceiver();
				if (channelSenderReceiver != null && channelSenderReceiver.isConnected()) {
					if (channelSenderReceiver.getTimeout() > 0
							&& System.currentTimeMillis() > channelSenderReceiver.getLastUsed()
									+ channelSenderReceiver.getTimeout()) {
						logger.info("Timed out from channel in " + sendingReceivingClient.getHost() + ":"
								+ sendingReceivingClient.getPort());
						return true;
					}
				}

			}
		}
		return false;

	}

	public ChannelSenderReceiver getChannelSenderReceiver() {
		return channelSenderReceiver;
	}

	public ProcessBuffer getProcessBuffer() {
		return processBuffer;
	}

	public ByteBuffer getReadingBuffer() {
		return readingBuffer;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public Boolean isRunning() {
		return this.isRunning.get();
	}
	
	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	private class ClientsTask implements Runnable {
		@Override
		public void run() {
			try {
				while (SendingReceivingClient.this != null
						&& SendingReceivingClient.this.getChannelSenderReceiver().isConnected()
						&& SendingReceivingClient.this.selector.isOpen() && !SendingReceivingClient.this.isTimeout()) {

					if ((SendingReceivingClient.this.selector.isOpen() && selector.select(1 * 1000) > 0)) {
						Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
						while (keys.hasNext()) {
							SelectionKey selectionKey = keys.next();
							keys.remove();
							if (selectionKey.isValid()) {
								if (selectionKey.isAcceptable()) {
									// nop
								} else if (selectionKey.isReadable()) {
									this.read(selectionKey);
								} else if (selectionKey.isConnectable()) {
									// nop
								} else if (selectionKey.isWritable()) {
									// TODO
								}
							}
						}
					} else {
						logger.debug("Nothing to do");
					}
				}
			} catch (Exception e) {
				logger.error("Exception in socket listener loop", e);
			}

			logger.debug("Exited Listener loop.");
			SendingReceivingClient.this.stop();
		}

		private void read(SelectionKey selectionKey) throws IOException {
			SendingReceivingClient sendingReceivingClient = (SendingReceivingClient) selectionKey.attachment();
			sendingReceivingClient.read();
		}
	}

}
