# Treasure Data Logger for Java

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, collecting these logs easily and 
reliably is a challenging task.

Treasure Data Logger solves the problem by having: easy installation, small 
footprint, plugins reliable buffering, log forwarding, etc.

  * Treasure Data website: [http://treasure-data.com/](http://treasure-data.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

**td-logger-java** is a Java library, to record events from Java application.

## Requirements

Java >= 1.6

## Install

### Install with all-in-one jar file

You can download all-in-one jar file for Treasure Data Logger.

    $ wget http://treasure-data.com/releases/java/td-logger-${logger.version}-jar-with-dependencies.jar

To use Treasure Data Logger for Java, set the above jar file to your classpath.

### Install from Maven2 repository

Treasure Data Logger for Java is released on Treasure Data's Maven2 repository.
You can configure your pom.xml as follows to use it:

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
        <id>treasure-data.com</id>
        <name>Treasure Data's Maven2 Repository</name>
        <url>http://treasure-data.com/maven2</url>
      </repository>
    <repositories>

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-logger-java.git
    $ cd td-logger-java
    $ mvn package

You will get the td-logger jar file in td-logger-java/target 
directory.  File name will be td-logger-${logger.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

**Replace ${logger.version} with the current version of TD Logger for Java.**
**The current version is 0.1.0.**  

## Quickstart

The following program is a small example of Treasure Data Logger for Java.

    import java.io.IOException;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.Properties;
    import com.treasure_data.logger.TreasureDataLogger;

    public class Main {
        private static TreasureDataLogger LOG;

        static {
            try {
                Properties props = System.getProperties();
                props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
                LOG = TreasureDataLogger.getLogger("mydatabase");
            } catch (IOException e) {
                // do something
            }
        }

        public void doApplicationLogic() {
            // ...
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("from", "userA");
            data.put("to", "userB");
            LOG.log("follow", data);
            // ...
        }
    }

To create Treasure Data Logger instances, users need to invoke getLogger method 
in Treasure Data Logger class like well-known logging libraries.  The method
should be called only once.

Before executing the Main program, you need to set the treasure-data.properties
file to your classpath.  Information of your API key should be described in the
file like this.

    td.logger.api.key=<your API key>

Close method in TreasureDataLogger class should be called explicitly when 
application is finished.

    TreasureDataLogger.close();

## License

Apache License, Version 2.0
