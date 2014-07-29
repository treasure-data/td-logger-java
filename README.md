# Treasure Data Logger for Java

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, collecting these logs easily and
reliably is a challenging task.

Treasure Data Logger solves the problem by having: easy installation, small
footprint, plugins reliable buffering, log forwarding, etc.

  * Treasure Data website: [http://treasuredata.com/](http://treasure-data.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

**td-logger-java** is a Java library, to record events from Java application. It supports two modes of operation: direct and indirect upload (through a td-agent, learn more [here](http://help.treasuredata.com/customer/portal/topics/550539-installing-td-agent/articles)).

This library leverages the [Treasure Data Java Client library, **td-client-java**](https://github.com/treasure-data/td-client-java) in direct upload mode and the [Fluentd Java Logger library, **fluent-logger-java**](https://github.com/fluent/fluent-logger-java) in indirect upload mode.

## Requirements

Java >= 1.6

## Install

### Install with all-in-one jar file

You can download all-in-one jar file for Treasure Data Logger.

    $ wget http://central.maven.org/maven2/com/treasuredata/td-logger/${logger.version}/td-logger-${logger.version}-jar-with-dependencies.jar

To use Treasure Data Logger for Java, set the above jar file to your classpath.

### Install from Maven repository

Treasure Data Logger for Java is released on Treasure Data's Maven repository.
You can configure your pom.xml as follows to use it:

    <dependencies>
      ...
      <dependency>
        <groupId>com.treasuredata</groupId>
        <artifactId>td-logger</artifactId>
        <version>${logger.version}</version>
      </dependency>
      ...
    </dependencies>

    <repositories>
      <repository>
        <id>fluentd.org</id>
        <name>Fluentd's Maven Repository</name>
        <url>http://fluentd.org/maven2</url>
      </repository>
    </repositories>

### Install with SBT (Build tool Scala)

To install td-logger From SBT (a build tool for Scala), please add the following lines to your build.sbt.

    /* in build.sbt */
    // Repositories
    resolvers ++= Seq(
      "fluent-logger Maven Repository" at "http://fluentd.org/maven2/"
    )
    // Dependencies
    libraryDependencies ++= Seq(
      "com.treasure_data" % "td-logger" % "${logger.version}"
    )

### Install from GitHub repository

You can get latest source code using git.

    $ git clone https://github.com/treasure-data/td-logger-java.git
    $ cd td-logger-java
    $ mvn package

You will get the td-logger jar file in td-logger-java/target
directory.  File name will be td-logger-${logger.version}-jar-with-dependencies.jar.
For more detail, see pom.xml.

**Replace ${logger.version} with the current version of Treasure Data Logger for Java.**
**The current version is 0.2.0.**

## Configuration

This logging library provides a variety of configuration options as Java System properties.
These options can be specified on the comamnd-line or using a properties file and loading it in the system properties at runtime.

    Properties props = System.getProperties();
    props.load(Main.class.getClassLoader().getResourceAsStream("treasure-data.properties"));

The current list of supported properties and their significance follows.

### Direct vs Indirect Upload

To select indirect upload through td-agent, please set the `td.logger.agentmode` property to 'true'.
For direct upload please set the property to 'false'.

The default value is `true`.

Also note that if the `TREASURE_DATA_API_KEY` environment variable is set, `td.logger.agentmode = false` will be set by default.

If you want to directly upload event logs from your application, you should set your own values to the following properties:

    td.logger.agentmode=false
    td.logger.api.key=<your API key>

You also can configure api endpoint like following:

    td.logger.agentmode=false
    td.logger.api.key=<your API key>
    td.logger.api.server.scheme=https://
    td.logger.api.server.host=api.treasuredata.com
    td.logger.api.server.port=443

On the other hand if you want to upload data via td-agent, you should declare the following properties:

    td.logger.agentmode=true
    td.logger.agent.host=<your td-agent host>
    td.logger.agent.port=<your td-agent port>

See below for the description of each of these properties.

### Agent's Host and Port

These two propertie set the location and port the Treasure Agent can be reached by: `td.logger.agent.host` and `td.logger.agent.port`.

Their default value is `localhost` and `24224` respectively.

### Agent's Tag Prefix

Treasure Agent marks the logging events using a 'tag', which is formed by a series of alphanumeric strings separated by a period character: e.g. 'string1.string2.string3'. Events created this logger are tagged with the database and table name the events are expected to be imported to. On the receiving end, the Treasure Agent, the database and table name will be used to determine the final database and table destination in the Treasure Data Cloud.

The `td.logger.agent.tag` properties provides the ability to set a fixed prefix for the tag that will be automatically added to all events logged by the library. For example, if events are destined to the 'mydb' database, and 'mytable' table within it, setting the `td.logger.agent.tag` property to `mycomp` will cause the tag to become `mycomp.mydb.mytable` as opposed to the default `td.mydb.mytable`. This is very important as the tag matching on the Treasure Agent side will need to be `mycomp.*.*` as opposed to the standard `td.*.*`.

The default value is `td`.

### Agent's Connection Timeout

The connection timeout can be set with the `td.logger.agent.timeout` property. The value is in seconds.

The default value is `3000` seconds.

### Agent's Buffer Capacity

The agent's maximum buffering capacity can be set using the `td.logger.agent.buffercapacity` property. The value is in bytes.

The default value is `1048576` bytes (1 MB).

### REST APIs Key

In order for the logger to be able to authenticate with your account in the Treasure Data Cloud, the REST APIs require an API key. The key can be provided by the `td.logger.api.key` property.

This property does not have a default value.

Alternatively the API key can be provided via the `TREASURE_DATA_API_KEY` environment variable. The environment variable takes precedence over the `td.logger.api.key` property.

Please note that we recommend to use a write-only API key. To obtain one, please:

1. Login into the Treasure Data Console at http://console.treasuredata.com;
2. Visit your Profile page at http://console.treasuredata.com/users/current;
3. Insert your password under the 'API Keys' panel;
4. In the bottom part of the panel, under 'Write-Only API keys', either copy the API key or click on 'Generate New' and copy the new API key.

### REST APIs Server Host and Port

The Treasure Data REST APIs Server host and port can be specified using the `td.logger.api.server.host`, `td.logger.api.server.port`, `td.logger.api.server.scheme` properties.

Their default values are `api.treasuredata.com` and `80` respectively.

Alternatively the REST APIs Server can be specified with the `TD_API_SERVER` environment variable. The value of the environment variable is read by the 'td-client-java' library directly and takes precedence over all other properties for both 'td-logger-java' and 'td-client-java' (`td.api.server.host`, `td.api.server.port`, and `td.api.server.scheme`).

### Auto Create

Both in case of Direct or Indirect Upload the system is able to create the destination table and/or database should they not already exist. This capability is disabled by default but can be enabled by setting the `td.logger.create.table.auto` to `true`.

Please note that depending on the permissions associated with the API key in use (either the one provided for direct upload or the one configured for the td-agent) databases may not be allowed to be created. Please see this [Access control documentation](http://docs.treasuredata.com/articles/access-control) page for more information.

## Quickstart

### Small example with Treasure Data Logger

The following program is a small example of td-logger.

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
                LOG = TreasureDataLogger.getLogger("my_database");
            } catch (IOException e) {
                // do something
            }
        }

        public void doApp() {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("from", "userA");
            data.put("to", "userB");
            LOG.log("my_follow_table", data);
        }
    }

See static initializer in Main class.  To create TreasureDataLogger objects,
you need to invoke getLogger method in TreasureDataLogger class like
well-known logging libraries.  The method should be called only once.

The close method in the TreasureDataLogger class should be called explicitly
when your application is finished (or unloaded). Once the method is executed,
all TreasureDataLogger objects that you created are closed.

    TreasureDataLogger.close();

See doApp method in the Main class. The log method enables you to upload event logs.
Event logs should be declared as variables of Map\<String, Object\> type.
The key is String type.  The type of value is one of the followings: int,
long, string, float, double.

## License

Apache License, Version 2.0
