package org.apache.flume.interceptor;

import static org.apache.flume.interceptor.GrokInterceptor.Constants.BODY_FIELD_NAME;
import static org.apache.flume.interceptor.GrokInterceptor.Constants.ENTRIES_PREFIX;
import static org.apache.flume.interceptor.GrokInterceptor.Constants.EXPRESSION;
import static org.apache.flume.interceptor.GrokInterceptor.Constants.OVERRIDE;
import static org.apache.flume.interceptor.GrokInterceptor.Constants.PATTERNS_DIR;
import static org.apache.flume.interceptor.GrokInterceptor.Constants.SOURCE;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.aicer.grok.dictionary.GrokDictionary;
import org.aicer.grok.util.Grok;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class GrokInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory.getLogger(GrokInterceptor.class);

  private final List <GrokHandler> grokHandlers;

  public GrokInterceptor(List <GrokHandler> grokHandlers) {
    this.grokHandlers = grokHandlers;
  }

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {

    for(GrokHandler grokHandler : grokHandlers) {
      grokHandler.handleEvent(event);
    }

    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    logger.info("Intercepting " + events.size() + " events");
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
  }

  public static class Builder implements Interceptor.Builder {

    private final List <GrokHandler> grokHandlers = new ArrayList<GrokHandler>();

    @Override
    public void configure(Context context) {

      boolean globalAllowOverrides = false;
      String patternsDirectory = null;

      if (StringUtils.isNotBlank(context.getString(OVERRIDE))) {
        globalAllowOverrides = context.getBoolean(OVERRIDE, false);
      }

      if (StringUtils.isNotBlank(context.getString(PATTERNS_DIR))) {
        patternsDirectory = context.getString(PATTERNS_DIR);
      }

      final GrokProcessor pWithOverride = new GrokProcessorWithOverride();
      final GrokProcessor pWithoutOverride = new GrokProcessorWithoutOverride();
      final GrokDictionary dictionary = new GrokDictionary();

      // Loads the built in dictionaries from the class path
      dictionary.addBuiltInDictionaries();

      // Loads any user specified patterns directory
      if (patternsDirectory != null) {
        dictionary.addDictionary(new File(patternsDirectory));
      }

      // Finalize the GrokDictionary object used before compiling expressions
      dictionary.bind();

      int currentGrokEntryIndex = 0;
      String currentIndexPrefix = ENTRIES_PREFIX + currentGrokEntryIndex + ".";
      boolean keepProcessingEntries = StringUtils.isNotBlank(context.getString(currentIndexPrefix + SOURCE));

      Preconditions.checkState(keepProcessingEntries, "Missing Grok expression entries. At least one is required");

      while (keepProcessingEntries) {

        final Context entryContext = new Context(context.getSubProperties(currentIndexPrefix));

        Preconditions.checkState(StringUtils.isNotBlank(entryContext.getString(SOURCE)),
            "Missing Param for entry index (" + currentIndexPrefix + ") " + SOURCE);

        Preconditions.checkState(StringUtils.isNotBlank(entryContext.getString(EXPRESSION)),
            "Missing Param for entry index (" + currentIndexPrefix + ") " + EXPRESSION);

        final boolean allowOverride = entryContext.getBoolean(OVERRIDE, globalAllowOverrides);
        final GrokProcessor processor = (allowOverride) ? pWithOverride : pWithoutOverride;
        final String sourceField = entryContext.getString(SOURCE);
        final String expression = entryContext.getString(EXPRESSION);

        final GrokHandler grokHandler = new GrokHandler(dictionary.compileExpression(expression), sourceField, processor);

        grokHandlers.add(grokHandler);

        // Setting things up for the next iteration
        currentGrokEntryIndex++;
        currentIndexPrefix = ENTRIES_PREFIX + currentGrokEntryIndex + ".";
        keepProcessingEntries = StringUtils.isNotBlank(context.getString(currentIndexPrefix + SOURCE));
      }

      Preconditions.checkState(grokHandlers.size() > 0,
          "GrokInterceptor must have at least one valid entry. Entries are sequentially numbered starting with index 0");

    }

    @Override
    public Interceptor build() {
      return new GrokInterceptor(grokHandlers);
    }

  }

  public static class GrokHandler {

    private final GrokProcessor processor;
    private final String sourceFieldName;
    private final Grok grok;

    public GrokHandler(Grok grok, String sourceFieldName, GrokProcessor processor) {
      this.grok = grok;
      this.sourceFieldName = sourceFieldName;
      this.processor = processor;
    }

    /**
     * Handles the Data Extraction from the Headers <p>
     *
     * Uses the GrokProcessor to extract
     * @param event
     */
    public void handleEvent(final Event event) {

        final Map <String, String> headers = event.getHeaders();
        final String rawDataInstance = !sourceFieldName.equals(BODY_FIELD_NAME) ? headers.get(sourceFieldName) : new String(event.getBody());

        if (event == null || rawDataInstance == null) {
          return;
        }

        processor.process(event, grok, rawDataInstance);
    }
  }

  /**
   * Extracts Named Groups from Raw Data <p>
   *
   * This processor overrides the field in the header if it already exists
   *
   */
  public static class GrokProcessorWithOverride implements GrokProcessor {

    @Override
    public void process(final Event event, final Grok expr, final String data) {

      Map <String, String> eventHeaders = event.getHeaders();

      Map <String, String> namedGroups = expr.extractNamedGroups(data);

      for (Map.Entry<String, String> namedGroup : namedGroups.entrySet()) {
        eventHeaders.put(namedGroup.getKey(), namedGroup.getValue());
      }
    }

  }

  /**
   * Extracts Named Groups from Raw Data <p>
   *
   * This processor does not override the field in the header if it already exists
   *
   */
  public static class GrokProcessorWithoutOverride implements GrokProcessor {

    @Override
    public void process(final Event event, final Grok expr, final String data) {

      Map <String, String> eventHeaders = event.getHeaders();

      Map <String, String> namedGroups = expr.extractNamedGroups(data);

      for (Map.Entry<String, String> namedGroup : namedGroups.entrySet()) {

        if (!eventHeaders.containsKey(namedGroup.getKey())) {

          eventHeaders.put(namedGroup.getKey(), namedGroup.getValue());
        }
      }
    }

  }

  /**
   * GrokProcessor used to do the actual extraction of named regex groups
   *
   */
  public static interface GrokProcessor {

    /**
     * Processes the Sample Data and Injects the extracted groups in the Event
     *
     * @param event Flume Event
     * @param expr Grok Expression
     * @param data Data to be processed by grok expression
     */
    public void process(final Event event, final Grok expr, final String data);

  }

  /**
   * Constants used by GrokInterceptor
   *
   */
  public static class Constants {

    public static final String BODY_FIELD_NAME = "@body";
    public static final String SOURCE = "source";
    public static final String PATTERNS_DIR = "patterns_dir";
    public static final String EXPRESSION = "expression";
    public static final String OVERRIDE = "override";
    public static final String ENTRIES = "entries";
    public static final String ENTRIES_PREFIX = "entries.";
  }
}
