CLASSPATH=./target/classes:~/.m2/repository/com/almworks/sqlite4java/sqlite4java/0.282/sqlite4java-0.282.jar:~/.m2/repository/com/cloudant/mazha/0.1-SNAPSHOT/mazha-0.1-SNAPSHOT.jar:~/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.1.1/jackson-core-2.1.1.jar:~/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.1.1/jackson-databind-2.1.1.jar:~/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.1.1/jackson-annotations-2.1.1.jar

export CLASSPATH
groovysh -Dsqlite4java.library.path=target/lib/ 
