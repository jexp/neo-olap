. ./settings.sh

mvn clean test-compile exec:java -Dexec.mainClass=org.neo4j.olap.Runner -Dexec.classpathScope=test -Dexec.args=$0