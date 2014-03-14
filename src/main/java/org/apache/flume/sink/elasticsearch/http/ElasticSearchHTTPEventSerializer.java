package org.apache.flume.sink.elasticsearch.http;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import com.google.gson.JsonObject;

public interface ElasticSearchHTTPEventSerializer extends Configurable {

  public static final Charset charset = Charset.defaultCharset();

  abstract public JsonObject serialize(Event event);
}
