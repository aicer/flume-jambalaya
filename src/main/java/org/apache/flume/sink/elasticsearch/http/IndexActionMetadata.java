package org.apache.flume.sink.elasticsearch.http;

import java.util.HashMap;
import java.util.Map;

public class IndexActionMetadata {

  private final Map<String, String> index = new HashMap<String, String>();

  public IndexActionMetadata(final String index, final String type) {
    this.setIndex(index).setType(type);
  }

  public IndexActionMetadata setIndex(final String value) {
    this.index.put("_index", value);
    return this;
  }

  public IndexActionMetadata setType(final String value) {
    this.index.put("_type", value);
    return this;
  }

}
