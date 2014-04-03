package org.apache.flume.interceptor;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.event.SimpleEvent;

public class GrokStage {

  public static final void main(String[] args) {

  /**
    jambalaya.sources.s1.interceptors = grok
    jambalaya.sources.s1.interceptors.grok.type = org.apache.flume.interceptor.GrokInterceptor$Builder
    jambalaya.sources.s1.interceptors.grok.override = false

    jambalaya.sources.s1.interceptors.grok.entries.0.source = @body
    jambalaya.sources.s1.interceptors.grok.entries.0.expression = %{USERNAME:username} was born on %{INT:yearOfBirth}
    jambalaya.sources.s1.interceptors.grok.entries.0.override = true

    jambalaya.sources.s1.interceptors.grok.entries.1.source = @body
    jambalaya.sources.s1.interceptors.grok.entries.1.expression = %{USERNAME:userid} is %{INT:age} years old
    jambalaya.sources.s1.interceptors.grok.entries.1.override = false
  */

    Map<String, String> config = new HashMap<String, String>();

    config.put("override", "false");

    config.put("entries.0.source", "@body");
    config.put("entries.0.expression", "%{USERNAME:username} was born on %{INT:yearOfBirth}");
    config.put("grok.entries.0.override", "false");

    config.put("entries.1.source", "@body");
    config.put("entries.1.expression", "%{USERNAME:userid} is %{INT:age} years old");
    config.put("grok.entries.1.override", "false");

    Context context = new Context(config);

    Interceptor.Builder builder = new org.apache.flume.interceptor.GrokInterceptor.Builder();
    builder.configure(context);

    Interceptor grokInterceptor = builder.build();

    SimpleEvent event = new SimpleEvent();
    Map<String, String> eventHeaders = event.getHeaders();

    eventHeaders.put("fullName", "Israel Ekpo");
    eventHeaders.put("tag", "google");
    event.setBody("iekpo was born on 1930 - jekpo is 35 years old today ".getBytes());

    grokInterceptor.intercept(event);

    for (Map.Entry<String, String> headerEntry : event.getHeaders().entrySet()) {
      System.out.println(headerEntry.getKey() + "=" + headerEntry.getValue());
    }

    System.out.println(new String(event.getBody()));
  }
}
