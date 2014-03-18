### Flume Jambalaya ###

According to the Oxford English Dictionary, in terms of its etymology, the word Jambalaya originates from the Provencal word 'Jambalaia', which means a mish mash, hodge-podge or mixture of diverse elements. Provencal is a dialect of Occitan spoken primarily by a minority of people in southern France, mostly in Provence.

As the name suggests, Flume Jambalaya is a standalone Apache Flume plugin that contains a variety of sources, interceptors, channels, sinks, serializers and other components designed to extend the Flume architecture.

Like other plugins designed for Apache Flume version 1.4.0 or later, Flume Jambalaya is designed so that it can be packaged with 3 main directories as follows:

* lib - the plugin’s jar(s)
* libext - its dependency jar(s)
* native - any required native libraries, such as .so files, if any

These sub-directories will be inside the "flume-jambalaya" directory which is inside the "plugins.d" directory for the Flume installation.

The components or features from Flume Jambalaya could be coverted to permanent components of the Apache Flume project and submitted for inclusion in the Apache Flume project when they are mature enough and thouroughly tested.

For now, feel free to send any feedback you may have.

Thank you.

### How to Compile the Plugin ###

To compile the jamabalya plugin simply checkout the source from github and then use Maven to compile it.

Once the jar files have been successfully generated, then use the plugin generator script to package and install the plugin within your Flume installation

This guide assumes that Apache Flume was installed in the /opt/flume.

If your installation directory is not in /opt/flume you will have to modify the value for FLUME_HOME in the shell script "generate-plugin.sh"

Example configuration files for how to set up the Flume agents are available in the "sample-configuration-files" directory

Here is how to get and install the plugin

```shell

$ git clone https://github.com/aicer/flume-jambalaya

$ cd flume-jambalaya

$ mvn clean package

# This assembles all the dependencies and installs the plugin in the $FLUME_HOME/plugins.d directory

$ ./generate-plugin.sh

```
### File Source ###

The Jambalaya plugin contains a source named FileSource.

This source lets you ingest data by tailing files from a specific path. 

Currently, it is only able to watch one file at a time.

This source will watch the specified file, and will parse events out of the file as they are appended to the log file.

It also continues to watch the file event after it is rotated.

Here is a sample configuration showing how the different options can be specified

```shell

# Since this is a custom source, you will have to specify the FQCN for the source.
# Configuring the Sources (Our custom source)
jambalaya.sources.s1.type = org.apache.flume.source.file.FileSource

# Binding the Channels to the Source
jambalaya.sources.s1.channels = c1

# Specifying the path configuration option for the custom source
jambalaya.sources.s1.path = /home/iekpo/Documents/MassiveLogData/date-monitor.log

# Specifying the delay between checks of the file for new content in milliseconds
jambalaya.sources.s1.delayMillis = 100

# Set to true to tail from the end of the file, false to tail from the beginning of the file
jambalaya.sources.s1.startFromEnd = true

```


### ElasticSearch HTTP Sink ###


This sink sends events to an ElasticSearch cluster via HTTP.

The events are constructed in a format that is compatible for display with the Kibana graphical interface.

Since the ElasticSearch binary client is not used, you can use any version of ElasticSearch on the server that is compatible with the API specifications assumed by the client.

You no longer have to match the major version of the client JAR with that of the server and ensure that both are running the same minor version of the JVM. 

SerializationExceptions used to occur if this is not the case.

Events will be written to a new index every day. 

The name will be <indexName>-yyyy-MM-dd where <indexName> is the indexName parameter. 

The sink will start writing to a new index at midnight UTC.

The type for this sink is the FQCN org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSink

Here is a sample configuration file for the sink

```shell

# Binding the Channels to the Sink
jambalaya.sinks.k1.channel = c1

# Configuring which sink the events are going out to
jambalaya.sinks.k1.type = org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPSink

# The hostname of the ElasticSearch server
jambalaya.sinks.k1.hostName = localhost

# The HTTP port number for the ElasticSearch server
jambalaya.sinks.k1.port = 9200

# The index prefix on the ElasticSearch server
jambalaya.sinks.k1.indexName = flume

# The mapping within the index
jambalaya.sinks.k1.indexType = log

# The maximum number of events sent to the ElasticSearch server per transaction
jambalaya.sinks.k1.batchSize = 32

# The serializer that converts Flume events into JSON objects that are sent to the ElasticSearch server
jambalaya.sinks.k1.serializer = org.apache.flume.sink.elasticsearch.http.ElasticSearchHTTPDynamicEventSerializer

# The name of the body field for each log event sent to the server
jambalaya.sinks.k1.serializer.bodyFieldName = body

```


### Date Interceptor ###

The date interceptor is used for parsing dates from fields and using that date or timestamp as the timestamp for the Flume event.

The parsed date can then be injected into the 'timestamp' header for the Flume Event.

The type for this interceptor is the FQCN of the Builder


Here is a sample configuration for this interceptor

```
# The name of the interceptor
jambalaya.sources.s1.interceptors = datefilter

# The type for the interceptor
jambalaya.sources.s1.interceptors.datefilter.type = org.apache.flume.interceptor.DateInterceptor$Builder

# The source of the timestamp (the name of the field in the header)
jambalaya.sources.s1.interceptors.datefilter.source = logtime

# The destination field (by default it is the timestamp field)
jambalaya.sources.s1.interceptors.datefilter.destination = timestamp

# The Joda Time compatible date format
# http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html
jambalaya.sources.s1.interceptors.datefilter.dateFormat = yyyy-MM-dd HH:mm:ss

# The Joda Time compatible Timezone
# http://joda-time.sourceforge.net/timezones.html
jambalaya.sources.s1.interceptors.datefilter.timezone = America/Los_Angeles

# The locale Language (en by default)
jambalaya.sources.s1.interceptors.datefilter.locale.language = en

# The locale Country (US by default)
jambalaya.sources.s1.interceptors.datefilter.locale.country = US

```



### Pattern Extractor Interceptor ###

This interceptor is very useful in matching regular expressions within the header field or body of the event.

The pattern matched can then be extracted from the source and injected into the destination header field.

It is very useful for extracting dates from old log events in combination with the DateInterceptor.

The type for the interceptor is its FQCN org.apache.flume.interceptor.PatternExtractorInterceptor$Builder

Here is a sample configuration for the the pattern extractor interceptor


```
# The name of the interceptor
jambalaya.sources.s1.interceptors = grok

# The type for the interceptor
jambalaya.sources.s1.interceptors.grok.type = org.apache.flume.interceptor.PatternExtractorInterceptor$Builder

# The pattern to match in the source field
jambalaya.sources.s1.interceptors.grok.pattern = \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}

# The source of the pattern (the name of the field in the header) or the body of the event
# If no field name in the header is specified it matches it against the body of the event
jambalaya.sources.s1.interceptors.grok.source = body

# The destination field in the header
jambalaya.sources.s1.interceptors.grok.destination = logtime

```

