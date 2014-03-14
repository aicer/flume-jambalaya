package org.apache.flume.sink.elasticsearch.http;


public class ElasticSearchHTTPSinkConstants {

  public static final int DEFAULT_BATCH_SIZE = 100;

  public static final String BODY_FIELDNAME_KEY = "bodyFieldName";
  public static final String DEFAULT_BODY_FIELDNAME = "body";

  public static final String HOSTNAME = "hostName";
  public static final String DEFAULT_HOSTNAME = "localhost";

  public static final String PORT = "port";

  /**
   * The name to index the document to, defaults to 'flume'</p>
   * The current date in the format 'yyyy-MM-dd' will be appended to this name,
   * for example 'foo' will result in a daily index of 'foo-yyyy-MM-dd'
   */
  public static final String INDEX_NAME = "indexName";

  /**
   * The type to index the document to, defaults to 'log'
   */
  public static final String INDEX_TYPE = "indexType";

  /**
   * Maximum number of events the sink should take from the channel per
   * transaction, if available. Defaults to 100
   */
  public static final String BATCH_SIZE = "batchSize";

  /**
   * TTL in days, when set will cause the expired documents to be deleted
   * automatically, if not set documents will never be automatically deleted
   */
  public static final String TTL = "ttl";

  /**
   * The fully qualified class name of the serializer the sink should use.
   */
  public static final String SERIALIZER = "serializer";

  /**
   * Configuration to pass to the serializer.
   */
  public static final String SERIALIZER_PREFIX = SERIALIZER + ".";


  public static final int DEFAULT_PORT = 9200;
  public static final int DEFAULT_TTL = -1;
  public static final String DEFAULT_INDEX_NAME = "flume";
  public static final String DEFAULT_INDEX_TYPE = "log";

}
