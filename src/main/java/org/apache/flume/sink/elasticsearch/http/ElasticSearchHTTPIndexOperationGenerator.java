package org.apache.flume.sink.elasticsearch.http;

import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public interface ElasticSearchHTTPIndexOperationGenerator extends Configurable {

  public static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("Etc/UTC"));

  public static final Gson GSON = new GsonBuilder().create();

  public abstract ElasticSearchHTTPIndexOperation createIndexOperation(String indexPrefix, String indexType, Event event);
}
