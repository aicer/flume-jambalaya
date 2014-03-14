package org.apache.flume.sink.elasticsearch.http;

import org.apache.flume.Context;

public class IndexOperationGenerator extends AbstractElasticSearchIndexOperationGenerator {

  public IndexOperationGenerator(ElasticSearchHTTPEventSerializer eventSerializer) {
    super(eventSerializer);
  }

  @Override
  public void configure(Context context) {

  }
}
