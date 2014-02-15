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
