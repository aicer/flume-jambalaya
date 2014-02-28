package org.apache.flume.source.file;

public final class FileSourceConfigurationConstants {

  public static final String FILE_PATH = "path";

  public static final String START_FROM_END_KEY = "startFromEnd";

  public static final boolean START_FROM_END_DEFAULT = true;

  public static final String DELAY_MILLIS_KEY = "delayMillis";

  public static final int DELAY_MILLIS_DEFAULT = 100;

  public static final String BUFFER_SIZE_KEY = "bufferSize";

  public static final int BUFFER_SIZE_DEFAULT = 4096;

  public static final String REOPEN_KEY = "reOpen";

  public static final boolean REOPEN_DEFAULT = false;

}
