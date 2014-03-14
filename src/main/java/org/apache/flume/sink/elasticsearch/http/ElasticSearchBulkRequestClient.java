package org.apache.flume.sink.elasticsearch.http;

import org.aicer.hibiscus.http.client.HttpClient;

import com.google.gson.JsonObject;

public class ElasticSearchBulkRequestClient {

  private static final String DEFAULT_BULK_REQUEST_PATH = "/_bulk";

  private static final String DEFAULT_HOSTNAME = "localhost";

  private static final int DEFAULT_PORT = 9200;

  private static final String DEFAULT_HTTP_SCHEME = "http";

  private int size = 0;

  private final HttpClient client;

  private final StringBuilder sb;

  private String hostname = DEFAULT_HOSTNAME;

  private int port = DEFAULT_PORT;

  private String path = DEFAULT_BULK_REQUEST_PATH;

  private String scheme = DEFAULT_HTTP_SCHEME;

  public ElasticSearchBulkRequestClient() {

    this.client = new HttpClient();

    this.sb = new StringBuilder();
  }

  public ElasticSearchBulkRequestClient addIndexOperation(final ElasticSearchHTTPIndexOperation operation) {

    this.sb.append(operation.toString());
    this.size++;

    return this;
  }

  public int getNumberOfOperations() {
    return this.size;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getHostname() {
    return hostname;
  }

  public ElasticSearchBulkRequestClient setHostname(final String hostname) {
    this.hostname = hostname;
    return this;
  }

  public ElasticSearchHTTPBulkResponse execute() {

    final String url = this.scheme + "://" + this.hostname + ":" + this.port + this.path;

    this.client.addHeader("Content-Type", "application/json");

    this.client.setRawUrl(url);

    this.client.setRequestMethod(HttpClient.POST);

    this.client.setRequestBody(this.sb.toString());

    this.sb.setLength(0);
    this.size = 0;

    this.client.execute();

    return new ElasticSearchHTTPBulkResponse(client.getLastResponse());

  }

  // TODO - actually close the connection of the client
  public void close() {

  }

  public static void main(String[] args) {

    IndexActionMetadata metadata = new IndexActionMetadata("flume-2014-03-04", "log");

    JsonObject document1 = new JsonObject();

    document1.addProperty("married", true);
    document1.addProperty("age", 35);
    document1.addProperty("body", "This is the body of the message");

    JsonObject document2 = new JsonObject();

    document2.addProperty("_id", "lcophhgrQ2ityEixGEgQiQ");
    document2.addProperty("married", false);
    document2.addProperty("age", 44);
    document2.addProperty("body", "This is another document for Xerox body");

    ElasticSearchBulkRequestClient bulkRequest = new ElasticSearchBulkRequestClient();

    bulkRequest.addIndexOperation(new ElasticSearchHTTPIndexOperation(metadata, document1, ElasticSearchHTTPIndexOperationGenerator.GSON));
    bulkRequest.addIndexOperation(new ElasticSearchHTTPIndexOperation(metadata, document2, ElasticSearchHTTPIndexOperationGenerator.GSON));

    ElasticSearchHTTPBulkResponse r = bulkRequest.execute();

    if (r.hasFailures()) {
      System.out.println("\nError message\n");
      System.out.println(r.buildFailureMessage());
    }
  }



}
