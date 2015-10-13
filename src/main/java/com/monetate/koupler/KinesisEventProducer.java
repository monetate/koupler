package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class KinesisEventProducer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventProducer.class);
	public static final int DEFAULT_BACKOFF = 100; // (in ms)
	public static final int MAX_BACKOFF = 16000;
	private static int backOff = DEFAULT_BACKOFF;

	public static final int THROTTLE_ON_QUEUE_SIZE = 10000;

	public static boolean RUNNING = true;
	public static boolean VERBOSE = false;
	public static boolean PAUSED = false;
	public static final int SLEEP_TIME_WHEN_PAUSED = 10; // (in seconds)

	private static final Random RANDOM = new Random();
	public static KouplerMetrics METRICS;

	// Only used if spawned as thread.
	private BufferedReader bufferedReader;
	private KinesisProducer producer;
	private String streamName;

	public KinesisEventProducer() {
	}

	public KinesisEventProducer(String propertiesFile, String streamName) {
		this.streamName = streamName;
		KinesisProducerConfiguration config = KinesisProducerConfiguration.fromPropertiesFile(propertiesFile);
		this.producer = new KinesisProducer(config);
		METRICS = new KouplerMetrics(config);
	}

	private static final FutureCallback<UserRecordResult> CALLBACK = new FutureCallback<UserRecordResult>() {
		@Override
		public void onFailure(Throwable t) {
			if (t instanceof UserRecordFailedException) {
				Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
				LOGGER.error(
						String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
			}
			LOGGER.error("Exception during put", t);
		}

		@Override
		public void onSuccess(UserRecordResult result) {
			METRICS.ackEvent();
		}
	};

	public void setBufferedReader(BufferedReader bufferedReader) {
		this.bufferedReader = bufferedReader;
	}

	public static String getMonetateId(String line) {
		String[] record = line.split(",");
		StringBuilder sb = new StringBuilder();
		if (record.length >= 7) {
			return sb.append(record[4]).append(":").append(record[5]).append(":").append(record[6]).toString();
		} else {
			LOGGER.warn("Received line w/o enough fields to retrieve monetateId. [{}]", line);
			return null;
		}
	}

	/**
	 * When run as a thread, this will use the buffered reader with which this
	 * object was constructed.
	 */
	@Override
	public void run() {
		this.publishEventsFrom(bufferedReader);
	}

	public void publishEventsFrom(BufferedReader bufferedReader) {
		LOGGER.debug("Publishing events to stream from [{}]...", bufferedReader);
		try {
			while (RUNNING) {
				if (PAUSED) {
					LOGGER.debug("Tributary [PAUSED], sleeping for [{}] seconds", SLEEP_TIME_WHEN_PAUSED);
					Thread.sleep(SLEEP_TIME_WHEN_PAUSED * 1000);
				} else {
					if (producer != null && producer.getOutstandingRecordsCount() > THROTTLE_ON_QUEUE_SIZE) {
						LOGGER.warn(
								"Throttling ingest, waiting [{}] milliseconds because we are waiting on [{}] records.",
								backOff, this.producer.getOutstandingRecordsCount());
						Thread.sleep(backOff);
						backOff = Math.min(MAX_BACKOFF, backOff * 2) + RANDOM.nextInt(100);

					} else {
						String line = bufferedReader.readLine();
						if (line != null) {
							sendLine(line);
							backOff = DEFAULT_BACKOFF;
						} else {
							throw new RuntimeException("Received null line! Bad Framing, let's punt this socket");
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO: Should we die here?
			LOGGER.error("Error while processing fact stream...", e);
		}
	}

	public void sendLine(String line) throws UnsupportedEncodingException {
		if (VERBOSE)
			LOGGER.debug("Sending [{}]", line);
		byte[] bytes = line.getBytes("UTF-8");
		METRICS.queueEvent(bytes.length);
		ByteBuffer data = ByteBuffer.wrap(bytes);
		String monetateId = getMonetateId(line);
		if (monetateId != null) {
			ListenableFuture<UserRecordResult> f = producer.addUserRecord(streamName, monetateId, data);
			Futures.addCallback(f, CALLBACK);
		}
	}
}
