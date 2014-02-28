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

This source lets you ingest data by tailing files from a specific path. 

Currently, it is only able to watch one file at a time.

This source will watch the specified file, and will parse events out of the file as they are appended to the log file.

It also continues to watch the file event after it is rotated.

Here is a sample configuration showing how the different options can be specified

```shell

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
