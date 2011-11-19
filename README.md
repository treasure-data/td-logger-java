# TD Logger for Java

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, collecting these logs easily and 
reliably is a challenging task.

TD Logger solves the problem by having: easy installation, small footprint, plugins
reliable buffering, log forwarding, etc.

  * TD website: [http://github.com/treasure-data](http://github.com/treasure-data)

**td-logger-java** is a Java library, to record events from Java application.

## Requirements

Java >= 1.6

## Install

### Install with all-in-one jar file

You can download all-in-one jar file for TD Logger for Java.

    $ wget [http://fluentd.org/releases/java/fluent-logger-${logger.version}-jar-with-dependencies.jar](http://fluentd.org/releases/java/fluent-logger-${logger.version}-jar-with-dependencies.jar)

To use TD Logger for Java, set the above jar file to your classpath.

### Install from Maven2 repository

TD Logger for Java is released on Fluent Maven2 repository.  You can 
configure your pom.xml as follows to use it:

    <dependencies>
      ...
      <dependency>
        <groupId>com.treasure_data</groupId>
        <artifactId>td-logger</artifactId>
        <version>${logger.version}</version>
      </dependency>
      ...
    </dependencies>

    <repositories>
      <repository>
        <id>fluentd.org</id>
        <name>Fluent Maven2 Repository</name>
        <url>http://fluentd.org/maven2</url>
      </repository>
    <repositories>

### Install from Github repository

You can get latest source code using git.

    $ git clone git@github.com:treasure-data/td-logger-java.git
    $ cd td-logger-java
    $ mvn package

You will get the td logger jar file in td-logger-java/target 
directory.  File name will be td-logger-${logger.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

**Replace ${logger.version} with the current version of TD Logger for Java.**
**The current version is 0.1.0.**  

## Quickstart

The following program is a small example of Fluent Logger for Java.

    import java.util.HashMap;
    import java.util.Map;
    import org.fluentd.logger.FluentLogger;

    public class Main {
        private static FluentLogger LOG = FluentLogger.getLogger("app");

        public void doApplicationLogic() {
            // ...
            Map<String, String> data = new HashMap<String, String>();
            data.put("from", "userA");
            data.put("to", "userB");
            LOG.log("follow", data);
            // ...
        }
    }

To create Fluent Logger instances, users need to invoke getLogger method in 
FluentLogger class like org.slf4j, org.log4j logging libraries.  The method 
should be called only once.  By default, the logger assumes fluent daemon is 
launched locally.  You can also specify remote logger by passing the following 
options.  

  // for remote fluentd
  private static FluentLogger LOG = FluentLogger.getLogger("app", "remotehost", port);

Then, please create the events like this.  This will send the event to fluentd, 
with tag 'app.follow' and the attributes 'from' and 'to'.

Close method in FluentLogger class should be called explicitly when application 
is finished.  The method close socket connection with the fluentd.

  FluentLogger.close();

## License

Apache License, Version 2.0
