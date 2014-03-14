package org.apache.flume.sink.elasticsearch.http;

import org.apache.commons.lang.math.NumberUtils;


public class DataUtils {

  public static boolean isBoolean(final String value) {
    return ((value != null) && (value.equals("true") || value.equals("false")));
  }

  public static boolean isNumber(final String value) {
   return NumberUtils.isNumber(value);
  }
}
