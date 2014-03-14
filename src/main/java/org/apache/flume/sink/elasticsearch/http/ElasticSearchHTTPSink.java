package org.apache.flume.sink.elasticsearch.http;

import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_HOSTNAME;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_PORT;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.HOSTNAME;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.PORT;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.SERIALIZER;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.SERIALIZER_PREFIX;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * ElasticSearch HTTP Sink
 *
 * This was modeled after the ElasticSearchSink that uses the binary client
 *
 * This sink uses HTTP to transfer the events to the server so that we are not bound by matching server and client versions
 *
 * @author Israel Ekpo
 */
public class ElasticSearchHTTPSink extends AbstractSink implements Configurable {

  // Static logger for the Sink
  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchHTTPSink.class);

  // Metrics and data collection tools
  private SinkCounter sinkCounter;
  private final CounterGroup counterGroup = new CounterGroup();

  // Transfers the HTTP request to the ElasticSearch server using HTTP (non-secure)
  private ElasticSearchBulkRequestClient client;

  // Used to generate bulk request item (header and document)
  private ElasticSearchHTTPIndexOperationGenerator operationGenerator;

  // This is the maximum number of events to drain from the channel per transaction
  private int batchSize = DEFAULT_BATCH_SIZE;

  // This is the prefix for the index
  private String indexName = DEFAULT_INDEX_NAME;

  // This is the mapping for the index storing the log event
  private String indexType = DEFAULT_INDEX_TYPE;

  // This is the host name for the ElasticSearch installation
  private String hostName = DEFAULT_HOSTNAME;

  // This is the HTTP port
  private int port = DEFAULT_PORT;

  @Override
  public Status process() throws EventDeliveryException {

    logger.debug("processing outgoing events");

    // READY if 1 or more Events were successfully delivered
    // BACKOFF if no data could be retrieved from the channel feeding this sink
    Status status = Status.READY;

    // Retrieve the channel feeding this Sink
    Channel channel = getChannel();

    // Setting up transaction boundary during this access of the channel
    Transaction txn = channel.getTransaction();

    try {

      // Marks the beginning of the transaction boundary
      txn.begin();

      // Take as many as this.batchSize events
      for (int i = 0; i < batchSize; i++) {

        Event event = channel.take();

        // If there are no more events to take, stop the loop
        if (event == null) {
          break;
        }

        // Prepare the index operation containing the bulk item header and document to be indexed
        ElasticSearchHTTPIndexOperation indexOperation = operationGenerator.createIndexOperation(indexName, indexType, event);

        // Add the bulk request item to the queue
        client.addIndexOperation(indexOperation);

      } // end of for loop to extract events for this transaction batch

      // How many bulk items did we prepare
      int size = client.getNumberOfOperations();

      if (size <= 0) { // If we did not successfully extract any events

        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;

      } else { // If we had at least one event

        if (size < batchSize) { // If we got some events but not up to this.batchSize
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else { // if we did get up to the maximum number of events allowed per transaction
          sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(size);

        // Attempt to transmit bulk request to ElasticSearch server
        ElasticSearchHTTPBulkResponse bulkResponse = client.execute();

        // If one or more of the bulk items had a failure
        if (bulkResponse.hasFailures()) {
          final String failureMessage = bulkResponse.buildFailureMessage();
          logger.error("Failure Message: " + failureMessage);
          throw new EventDeliveryException(failureMessage);
        }
      }

      // Commit the transaction in progress
      txn.commit();

      sinkCounter.addToEventDrainSuccessCount(size);

      counterGroup.incrementAndGet("transaction.success");

    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        logger.error(
            "Exception in rollback. Rollback might not have been successful.",
            ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        Throwables.propagate(ex);
      } else {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        throw new EventDeliveryException(
            "Failed to commit transaction. Transaction rolled back.", ex);
      }
    } finally {
      // Marks the end of the transaction boundary for the current channel operation
      txn.close();
    }

    return status;
  }

  @Override
  public void configure(Context context) {

    if (StringUtils.isNotBlank(context.getString(HOSTNAME))) {
      this.hostName = context.getString(HOSTNAME);
    }

    if (StringUtils.isNotBlank(context.getString(PORT))) {
      this.port = Integer.parseInt(context.getString(PORT));
    }

    if (StringUtils.isNotBlank(context.getString(INDEX_NAME))) {
      this.indexName = context.getString(INDEX_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(INDEX_TYPE))) {
      this.indexType = context.getString(INDEX_TYPE);
    }

    if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
      this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
    }

    String serializerClazz = "org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPDynamicEventSerializer";

    if (StringUtils.isNotBlank(context.getString(SERIALIZER))) {
      serializerClazz = context.getString(SERIALIZER);
    }

    Context serializerContext = new Context();
    serializerContext.putAll(context.getSubProperties(SERIALIZER_PREFIX));

    try {

      @SuppressWarnings("unchecked")
      Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class.forName(serializerClazz);

      Configurable serializer = clazz.newInstance();

      if (serializer instanceof ElasticSearchHTTPEventSerializer) {

        operationGenerator = new IndexOperationGenerator((ElasticSearchHTTPEventSerializer) serializer);

      } else {

        throw new IllegalArgumentException(serializerClazz + " is not an ElasticSearchHTTPEventSerializer");
      }

      operationGenerator.configure(serializerContext);

    } catch (Exception e) {

      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }

    Preconditions.checkState(StringUtils.isNotBlank(indexName),
        "Missing Param:" + INDEX_NAME);
    Preconditions.checkState(StringUtils.isNotBlank(indexType),
        "Missing Param:" + INDEX_TYPE);

    Preconditions.checkState(batchSize >= 1, BATCH_SIZE
        + " must be greater than 0");

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("ElasticSearch HTTP sink {} started");
    sinkCounter.start();
    try {
      openConnection();
    } catch (Exception ex) {
      logger.error("An error occurred while attempting to open connection", ex);
      sinkCounter.incrementConnectionFailedCount();
      closeConnection();
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("ElasticSearch HTTP sink {} stopping");
    closeConnection();

    sinkCounter.stop();
    super.stop();
  }

  private void openConnection() {

    client = new ElasticSearchBulkRequestClient();

    client.setHostname(hostName);
    client.setPort(port);

    sinkCounter.incrementConnectionCreatedCount();
  }

  private void closeConnection() {

    if (this.client != null) {
      this.client.close();
      this.client = null;
    }

    sinkCounter.incrementConnectionClosedCount();
  }
}
