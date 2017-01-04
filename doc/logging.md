Logging
=====

This document explains how to configure [logging](http://docs.oracle.com/javase/7/docs/api/index.html?java/util/logging/) for sync-android.

By default sync-android does not set any logging settings, so the system's defaults are used.

There are two ways to configure `java.util.logging`. It can be configured via a properties file or programatically. 

### Configuring using the properties file

An example configuration for logging to the console:

```ini

# "handlers" specifies a comma separated list of log Handler 
# classes. For this example it will use only the Console Handler
# however there a other handlers such as FileHandler 
# see http://docs.oracle.com/javase/7/docs/api/java/util/logging/package-summary.html
# for a list of handlers provided by the JDK.
handlers = java.util.logging.ConsoleHandler

# Limit the messages that are printed on the console to INFO and above.
# Full list of levels http://docs.oracle.com/javase/7/docs/api/java/util/logging/Level.html
java.util.logging.ConsoleHandler.level = INFO

# Set the formatter for this handler, there are several default formatters 
# available, or you can create your own custom formatter
java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter

# Set cloudant sync classes to log at level FINE
com.cloudant.level = FINE

```

To load the properties into the log manager on demand, such as when clicking a button to enable logging

```java
LogManager manager = LogManager.getLogManager();
manager.readConfiguration(new FileInputStream("path/to/logging/config");
```
or to set up logging on application start up (Java SE)

```bash
$ java -jar myApplication.jar -Djava.util.logging.config.file=/path/to/logging/config
```

### Configuring the log manager programatically

An example on how to configure `java.util.logging` programatically to log to the console with level `INFO` or higher:

```java

//Create a handler in this case a ConsoleHandler
Handler consoleHandler = new ConsoleHandler();
consoleHandler.setLevel (Level.INFO);

//Get the target logger, in this case "com.cloudant" domain logger
Logger logger = Logger.getLogger("com.cloudant");
//add the handler to the logger
logger.addHandler (consoleHandler);

//set the logger to log messages with level FINE or higher
logger.setLevel (Level.FINE);
```
