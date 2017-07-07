# taildir

This is an extension of flume source that implements windows support for inode processing, 
support for different directory traversal, and processing of log4j log files. 
Here I have implemented a okhttp sink, and you should modify some logical code so that you can use it, and enjoy your use.

#Compilation

The project is maintained by Maven.

#Installation instructions

After your compilation, you should ship the target jar taildir-1.0.0.jar to the $FLUME_HOME/flume-ng/lib/. Then you can edit flume.conf to use the Taildir Source.

Now follows a brief overview of Taildir Source with usage instructions.

Sources

Taildir Source

The 'Taildir Source' is extended from original flume 1.7.0's Taildir Source, so you can use other properties just like using Taildir Source.

Example config:

agent1.sources=r1

agent1.sources.r1.type = org.apache.flume.source.taildir.TaildirSource
agent1.sources.r1.channels = channel1
agent1.sources.r1.positionFile = /Users/Downloads/apache-flume-1.7.0-bin/taildir_position.json
agent1.sources.r1.filegroups = f1
agent1.sources.r1.filegroups.f1 = /Users/Downloads/apache-flume-1.7.0-bin/*/.*log.*
agent1.sources.r1.headers.f1.headerKey1 = value1
agent1.sources.r1.fileHeader = true
agent1.sources.r1.byteOffsetHeader = true
agent1.sources.r1.fileHeaderKey = file

The last three configurations are customized for collecting yarn nodemanger's log to add headers. Of course, you can ignore and remove them.
