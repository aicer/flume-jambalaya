package org.apache.flume.sink.elasticsearch.http;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Represents an index operation
 *
 * Modeled after documentation available at <p>
 *
 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html
 *
 * @author Israel Ekpo
 *
 */
public class ElasticSearchHTTPIndexOperation {

  private final IndexActionMetadata metadata;

  private final JsonObject document;

  private final Gson gson;

  public ElasticSearchHTTPIndexOperation(final IndexActionMetadata metadata, final JsonObject document, final Gson gson) {
    this.metadata = metadata;
    this.document = document;
    this.gson = gson;
  }

  @Override
  public String toString() {

    String operation = gson.toJson(metadata, IndexActionMetadata.class) + "\n";

    operation += gson.toJson(document) + "\n";

    return operation;
  }

  public IndexActionMetadata getMetadata() {
    return metadata;
  }

  public JsonObject getDocument() {
    return document;
  }
}
