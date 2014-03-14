package org.apache.flume.sink.elasticsearch.http;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTimeUtils;

import com.google.common.collect.Maps;

public abstract class AbstractElasticSearchIndexOperationGenerator implements
    ElasticSearchHTTPIndexOperationGenerator {

  protected final FastDateFormat fastDateFormat;

  protected final ElasticSearchHTTPEventSerializer eventSerializer;

  public AbstractElasticSearchIndexOperationGenerator(ElasticSearchHTTPEventSerializer eventSerializer) {
    this(eventSerializer, ElasticSearchHTTPIndexOperationGenerator.DATE_FORMAT);
  }

  public AbstractElasticSearchIndexOperationGenerator(ElasticSearchHTTPEventSerializer eventSerializer, FastDateFormat fastDateFormat){
    this.fastDateFormat = fastDateFormat;
    this.eventSerializer = eventSerializer;
  }

  @Override
  public abstract void configure(Context context);

  @Override
  public ElasticSearchHTTPIndexOperation createIndexOperation(
      final String indexPrefix, final String indexType, final Event event) {

    final TimestampedEvent timestampedEvent = new TimestampedEvent(event);
    final long timestamp = timestampedEvent.getTimestamp();
    final String indexName = getIndexName(indexPrefix, timestamp);

    final IndexActionMetadata metadata = new IndexActionMetadata(indexName, indexType);

    return new ElasticSearchHTTPIndexOperation(metadata, eventSerializer.serialize(timestampedEvent), GSON);
  }

  /**
   * Gets the name of the index to use for an index request
   * @return index name of the form 'indexPrefix-formattedTimestamp'
   * @param indexPrefix
   *          Prefix of index name to use -- as configured on the sink
   * @param timestamp
   *          timestamp (millis) to format / use
   */
  protected String getIndexName(String indexPrefix, long timestamp) {
    return new StringBuilder(indexPrefix)
      .append('-')
      .append(fastDateFormat.format(timestamp)).toString();
  }


  final class TimestampedEvent extends SimpleEvent {

    private static final String ELASTICSEARCH_TIMESTAMP_FIELDNAME = "timestamp";
    private static final String KIBANA_TIMESTAMP_FIELDNAME = "@timestamp";

    private final long timestamp;

    TimestampedEvent(final Event originalEvent) {

      setBody(originalEvent.getBody());

      final Map<String, String> headers = Maps.newHashMap(originalEvent.getHeaders());

      String timestampString = headers.get(ELASTICSEARCH_TIMESTAMP_FIELDNAME);

      if (StringUtils.isBlank(timestampString)) {
        timestampString = headers.get(KIBANA_TIMESTAMP_FIELDNAME);
      }

      if (StringUtils.isBlank(timestampString)) {
        this.timestamp = DateTimeUtils.currentTimeMillis();
        headers.put(ELASTICSEARCH_TIMESTAMP_FIELDNAME, String.valueOf(timestamp ));
      } else {
        this.timestamp = Long.valueOf(timestampString);
      }

      setHeaders(headers);
    }

    long getTimestamp() {
        return timestamp;
    }
  }
}
