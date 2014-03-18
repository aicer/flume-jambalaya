package org.apache.flume.interceptor;

import static org.apache.flume.interceptor.PatternExtractorInterceptor.Constants.DESTINATION;
import static org.apache.flume.interceptor.PatternExtractorInterceptor.Constants.SOURCE;
import static org.apache.flume.interceptor.PatternExtractorInterceptor.Constants.PATTERN;
import static org.apache.flume.interceptor.PatternExtractorInterceptor.Constants.BODY_SOURCE;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;

import com.google.common.base.Preconditions;


public class PatternExtractorInterceptor implements Interceptor {

  private final Pattern pattern;
  private final String source;
  private final String destination;

  private PatternExtractorInterceptor(final Pattern pattern, final String source, final String destination) {
    this.pattern = pattern;
    this.source = source;
    this.destination = destination;
  }

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {
    return extractAndInject(event);
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {

  }

  private Event extractAndInject(final Event event) {

    String sourceString = "";
    String extractedValue = "";

    if (source.equals(BODY_SOURCE)) {
      sourceString = new String(event.getBody());
    } else {
      sourceString = event.getHeaders().get(source);
    }

    if (sourceString.length() > 0) {
      final Matcher matcher = pattern.matcher(sourceString);
      if (matcher.find()) {
        extractedValue = sourceString.substring(matcher.start(), matcher.end());
        event.getHeaders().put(destination, extractedValue);
      }
    }

    return event;
  }

  public static class Builder implements Interceptor.Builder {

    private Pattern pattern = null;
    private String destination = null;
    private String source = null;

    @Override
    public void configure(Context context) {

      String regexPattern = null;

      if (StringUtils.isNotBlank(context.getString(PATTERN))) {
          regexPattern = context.getString(PATTERN);
          pattern = Pattern.compile(regexPattern);
      }

      if (StringUtils.isNotBlank(context.getString(SOURCE))) {
          source = context.getString(SOURCE);
      }

      if (StringUtils.isNotBlank(context.getString(DESTINATION))) {
          destination = context.getString(DESTINATION);
      }

      Preconditions.checkState(StringUtils.isNotBlank(regexPattern),
          "Missing Param:" + PATTERN);

      Preconditions.checkState(StringUtils.isNotBlank(source),
          "Missing Param:" + SOURCE);

      Preconditions.checkState(StringUtils.isNotBlank(destination),
          "Missing Param:" + DESTINATION);
    }

    /**
     * Creates an instance of the Interceptor
     */
    @Override
    public Interceptor build() {
      return new PatternExtractorInterceptor(pattern, source, destination);
    }
  }

  public static class Constants {
    public static final String PATTERN = "pattern";
    public static final String DESTINATION  = "destination";
    public static final String SOURCE  = "source";
    public static final String BODY_SOURCE  = "body";
  }

}
