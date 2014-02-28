package org.apache.flume.source.file;

import static org.apache.flume.source.file.FileSourceConfigurationConstants.DELAY_MILLIS_DEFAULT;
import static org.apache.flume.source.file.FileSourceConfigurationConstants.DELAY_MILLIS_KEY;
import static org.apache.flume.source.file.FileSourceConfigurationConstants.FILE_PATH;
import static org.apache.flume.source.file.FileSourceConfigurationConstants.START_FROM_END_DEFAULT;
import static org.apache.flume.source.file.FileSourceConfigurationConstants.START_FROM_END_KEY;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileSource extends AbstractSource implements Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory.getLogger(FileSource.class);

  private static final int POLL_DELAY_MS = 500;

  private static final int INITIAL_DELAY_MS = 0;

  /* Configurable options for this source */
  private String filePath = null;
  private boolean startFromEnd = false;
  private long delayMillis = 100L;

  private ScheduledExecutorService fileTailerExecutor;
  private ScheduledExecutorService eventExtractorExecutor;

  private Queue<String> queue;
  private Semaphore mutex;
  private Tailer tailer;

  private SourceCounter sourceCounter;

  @Override
  public synchronized void configure(Context context) {

    this.filePath = context.getString(FILE_PATH, null);

    Preconditions.checkState(filePath != null, "Configuration must specify a source file path");

    this.delayMillis = context.getInteger(DELAY_MILLIS_KEY, DELAY_MILLIS_DEFAULT);
    this.startFromEnd = context.getBoolean(START_FROM_END_KEY, START_FROM_END_DEFAULT);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public synchronized void start() {

    logger.info("FileSource source {} starting.", getName());
    logger.info("FileSource source starting with file path: {}", filePath);

    this.queue = new LinkedList<String>();
    this.mutex = new Semaphore(1, true);

    this.fileTailerExecutor = Executors.newSingleThreadScheduledExecutor();
    this.eventExtractorExecutor = Executors.newSingleThreadScheduledExecutor();

    final FileSourceEventProducer listener = new FileSourceEventProducer();

    this.tailer = new Tailer(new File(filePath), listener, delayMillis, startFromEnd);
    FileSourceEventConsumer eventExtractorCommand = new FileSourceEventConsumer();

    this.fileTailerExecutor.scheduleWithFixedDelay(tailer, INITIAL_DELAY_MS, POLL_DELAY_MS, TimeUnit.MILLISECONDS);
    this.eventExtractorExecutor.scheduleWithFixedDelay(eventExtractorCommand, INITIAL_DELAY_MS, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {

    this.fileTailerExecutor.shutdown();

    try {
      this.fileTailerExecutor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.info("fileTailerExecutor Interrupted while awaiting termination", e);
    }

    this.fileTailerExecutor.shutdownNow();

    this.eventExtractorExecutor.shutdown();

    try {
      this.eventExtractorExecutor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.info("eventExtractorExecutor Interrupted while awaiting termination", e);
    }

    this.eventExtractorExecutor.shutdownNow();

    super.stop();
    sourceCounter.stop();

    logger.info("FileSource {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  private synchronized Event eventSerializer(final String rawEvent) {

    final Map<String,String> headers = new HashMap<String, String>();

    headers.put(FILE_PATH, filePath);

    final Event event = EventBuilder.withBody(rawEvent.getBytes(), headers);

    return event;
  }

  /**
   * Extracts the events from the queue
   *
   * The extracted events are sent down to the channel
   *
   * @author Israel Ekpo <israel@aicer.org>
   */
  private class FileSourceEventConsumer implements Runnable {

    @Override
    public void run() {

      while(true) {

        try {

          logger.debug("Acquiring Mutex in consumer");
          mutex.acquire();

          if (0 == queue.size()) {
            break;
          }

          try {

            sourceCounter.addToEventReceivedCount(1);
            sourceCounter.incrementAppendBatchReceivedCount();

            logger.debug("Sending event from Queue to Channel");
            getChannelProcessor().processEvent(eventSerializer(queue.peek()));

          } catch (ChannelException ce) {

            logger.warn("Channel is full at the moment. Releasing mutex from consumer");
            mutex.release();
            continue;
          }

          sourceCounter.addToEventAcceptedCount(1);
          sourceCounter.incrementAppendBatchAcceptedCount();

          // If the item was successfully processed then remove it
          logger.debug("Event processed successfully. Deleting from consumer queue");
          queue.poll();

        } catch (Throwable e) {

        } finally {

          logger.debug("Releasing Mutex from consumer");
          mutex.release();
        }
      }

      logger.debug("No more events in the queue. Shutting down consumer");

    }

  }


  /**
   * Listens to Events produced by the Tailer
   *
   * @author Israel Ekpo <israel@aicer.org>
   */
  public class FileSourceEventProducer implements TailerListener {

    /**
     * The tailer will call this method during construction,
     * giving the listener a method of stopping the tailer.
     * @param tailer the tailer.
     */
    @Override
    public void init(Tailer tailer) {

    }

    /**
     * This method is called if the tailed file is not found.
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     */
    @Override
    public void fileNotFound() {
      logger.warn("File " + tailer.getFile().getName() + " could not be found");
    }

    /**
     * Called if a file rotation is detected.
     *
     * This method is called before the file is reopened, and fileNotFound may
     * be called if the new file has not yet been created.
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     */
    @Override
    public void fileRotated() {
      logger.info("File " + tailer.getFile().getName() + " has been rotated");
    }

    /**
     * Handles a line from a Tailer.
     * <p>
     * <b>Note:</b> this is called from the tailer thread when a log event is detected
     * @param line the line.
     */
    @Override
    public void handle(String rawEvent) {

      logger.debug("Adding incoming event to the queue in producer");

      try {

        logger.debug("Acquiring Mutex for event queue in producer");
        mutex.acquire();

        logger.debug("Adding incoming event to the queue in producer");
        queue.add(rawEvent);

      } catch (Throwable e) {

      } finally {

        logger.debug("releasing Mutex from producer");
        mutex.release();
      }

    }

    /**
     * Handles an Exception .
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     * @param ex the exception.
     */
    @Override
    public void handle(Exception ex) {
      logger.error("File " + tailer.getFile().getName() + " encountered an exception while tailing");
      logger.error(ex.getMessage());
    }
  }
}
