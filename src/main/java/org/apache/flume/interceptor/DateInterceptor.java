package org.apache.flume.interceptor;

import static org.apache.flume.interceptor.DateInterceptor.Constants.DATE_FORMAT;
import static org.apache.flume.interceptor.DateInterceptor.Constants.DESTINATION;
import static org.apache.flume.interceptor.DateInterceptor.Constants.LOCALE_COUNTRY;
import static org.apache.flume.interceptor.DateInterceptor.Constants.LOCALE_LANGUAGE;
import static org.apache.flume.interceptor.DateInterceptor.Constants.LOCALE_PREFIX;
import static org.apache.flume.interceptor.DateInterceptor.Constants.LOCALE_VARIANT;
import static org.apache.flume.interceptor.DateInterceptor.Constants.SOURCE;
import static org.apache.flume.interceptor.DateInterceptor.Constants.TIMEZONE;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Date Interceptor
 *
 * Parses the date from another field into the timestamp field <p>
 *
 * Date patterns are available here <p>
 *
 * http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html <p>
 *
 * Valid timezone ids are available here <p>
 *
 * http://joda-time.sourceforge.net/timezones.html <p>
 *
 * @author iekpo
 *
 */
public class DateInterceptor implements Interceptor {

  //Static logger for the Interceptor
  private static final Logger logger = LoggerFactory.getLogger(DateInterceptor.class);

  private final String source;
  private final String destination;
  private final DateTimeFormatter formatter;

  private DateInterceptor(final String source, final String destination,
      final String dateFormat, final String timezone, final Locale locale) {
    this.source = source;
    this.destination = destination;

    formatter = org.joda.time.format.DateTimeFormat.forPattern(dateFormat).withDefaultYear(new DateTime().getYear());
    formatter.withZone(org.joda.time.DateTimeZone.forID(timezone));
    formatter.withOffsetParsed();
    formatter.withLocale(locale);
  }

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {
    return extractAndInsert(event);
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

  private Event extractAndInsert(final Event event) {

    final Map <String, String> headers = event.getHeaders();
    final String sourceValue = headers.get(source);

    if (StringUtils.isNotBlank(sourceValue) && sourceValue.length() > 0) {

      try {
        String timestampValue = formatter.parseMillis(sourceValue) + "";
        headers.put(destination, timestampValue);
      } catch (Exception e) {
        logger.error("Error while parsing date from {" + sourceValue + "}", e);
      }
    }

    return event;
  }

  public static class Builder implements Interceptor.Builder {

    private String source = null;
    private String destination = "timestamp";
    private String dateFormat = null;
    private String timezone = "Etc/UTC";
    private Locale locale;

    @Override
    public void configure(Context context) {

      if (StringUtils.isNotBlank(context.getString(SOURCE))) {
          source = context.getString(SOURCE);
      }

      if (StringUtils.isNotBlank(context.getString(DESTINATION))) {
          destination = context.getString(DESTINATION);
      }

      if (StringUtils.isNotBlank(context.getString(DATE_FORMAT))) {
          dateFormat = context.getString(DATE_FORMAT);
      }

      if (StringUtils.isNotBlank(context.getString(TIMEZONE))) {
          timezone = context.getString(TIMEZONE);
      }

      final Context localeContext = new Context();
      localeContext.putAll(context.getSubProperties(LOCALE_PREFIX));

      String localeLanguage = "en";
      String localeCountry = "US";
      String localeVariant = "";

      if (StringUtils.isNotBlank(localeContext.getString(LOCALE_LANGUAGE))) {
        localeLanguage = localeContext.getString(LOCALE_LANGUAGE);
      }

      if (StringUtils.isNotBlank(localeContext.getString(LOCALE_COUNTRY))) {
        localeCountry = localeContext.getString(LOCALE_COUNTRY);
      }

      if (StringUtils.isNotBlank(localeContext.getString(LOCALE_VARIANT))) {
        localeVariant = localeContext.getString(LOCALE_VARIANT);
      }

      locale = new Locale(localeLanguage, localeCountry, localeVariant);

      Preconditions.checkState(StringUtils.isNotBlank(dateFormat),
          "Missing Param:" + DATE_FORMAT);

      Preconditions.checkState(StringUtils.isNotBlank(source),
          "Missing Param:" + SOURCE);

      Preconditions.checkState(StringUtils.isNotBlank(destination),
          "Missing Param:" + DESTINATION);

      Preconditions.checkState(StringUtils.isNotBlank(timezone),
          "Missing Param:" + TIMEZONE);
    }

    @Override
    public Interceptor build() {
      return new DateInterceptor(source, destination, dateFormat, timezone, locale);
    }
  }

  public static class Constants {

    public static final String DATE_FORMAT = "dateFormat";
    public static final String DESTINATION  = "destination";
    public static final String SOURCE  = "source";
    public static final String TIMEZONE = "timezone";

    public static final String LOCALE_PREFIX = "locale.";
    public static final String LOCALE_LANGUAGE = "language";
    public static final String LOCALE_COUNTRY = "country";
    public static final String LOCALE_VARIANT = "variant";
  }
}
