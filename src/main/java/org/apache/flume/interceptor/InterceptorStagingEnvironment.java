package org.apache.flume.interceptor;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class InterceptorStagingEnvironment {

  public static final String PATTERN = "pattern";
  public static final String DESTINATION  = "destination";
  public static final String SOURCE  = "source";
  public static final String BODY_SOURCE  = "body";

  public static final String DATE_FORMAT = "dateFormat";
  public static final String TIMEZONE = "timezone";

  public static void main(String[] args) {

    final Event event = new SimpleEvent();

    byte[] body = "My date of birth in 1981 is 1991-06-28 12:35:51".getBytes();

    Map<String, String> headers = new HashMap<String, String>();

    event.setHeaders(headers);
    event.setBody(body);

    extractPattern(event);
    injectDateTime(event);
  }


  private static void injectDateTime(final Event event) {

    final Context context = new Context();

    context.put(TIMEZONE, "Pacific/Honolulu");
    context.put(SOURCE, "logtime");
    context.put(DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");

    Interceptor.Builder builder = new DateInterceptor.Builder();
    builder.configure(context);
    Interceptor interceptor = builder.build();
    interceptor.intercept(event);

    System.out.println(event.getHeaders().get("timestamp"));

    DateTime dt = new DateTime(Long.parseLong(event.getHeaders().get("timestamp"))).withZone(DateTimeZone.forID("UTC"));

    System.out.println(dt);

  }

  private static void extractPattern(final Event event) {

    final Context context = new Context();

    context.put(PATTERN, "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    context.put(DESTINATION, "logtime");
    context.put(SOURCE, "body");

    Interceptor.Builder builder = new PatternExtractorInterceptor.Builder();
    builder.configure(context);
    Interceptor interceptor = builder.build();
    interceptor.intercept(event);

    System.out.println(event.getHeaders().get("logtime"));
  }
}
