package org.apache.flume.sink.elasticsearch.http;

import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.BODY_FIELDNAME_KEY;
import static org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSinkConstants.DEFAULT_BODY_FIELDNAME;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.gson.JsonObject;

/**
 * ElasticSearch HTTP Dynamic Event Serializer
 *
 * Makes a best effort to serialize the events to the appropriate JSON types
 *
 * @author Israel Ekpo
 */
public class ElasticSearchHTTPDynamicEventSerializer implements ElasticSearchHTTPEventSerializer {

  private String bodyFieldName = DEFAULT_BODY_FIELDNAME;

  @Override
  public void configure(Context context) {

    if (StringUtils.isNotBlank(context.getString(BODY_FIELDNAME_KEY))) {
      bodyFieldName = context.getString(BODY_FIELDNAME_KEY);
    }
  }

  @Override
  public JsonObject serialize(Event event) {

    final JsonObject serializedEvent = new JsonObject();

    final Map <String, String> headers = event.getHeaders();

    serializedEvent.addProperty(bodyFieldName, new String(event.getBody()));

    for (String fieldName : headers.keySet()) {

      final String field = headers.get(fieldName);

      if (DataUtils.isNumber(field)) {
        serializedEvent.addProperty(fieldName, Double.parseDouble(field));
      } else if (DataUtils.isBoolean(field)) {
        serializedEvent.addProperty(fieldName, Boolean.parseBoolean(field));
      } else {
        serializedEvent.addProperty(fieldName, field);
      }

    }

    return serializedEvent;
  }
}
