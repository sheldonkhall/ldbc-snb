#!/bin/bash

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`
DEFAULT_HADOOP_HOME=$SCRIPTPATH/./hadoop-2.6.0 #change to your hadoop folder
DEFAULT_LDBC_SNB_DATAGEN_HOME=$SCRIPTPATH #change to your ldbc_socialnet_dbgen folder

# allow overriding configuration from outside via environment variables
# i.e. you can do
#     HADOOP_HOME=/foo/bar LDBC_SNB_DATAGEN_HOME=/baz/quux ./run.sh
# instead of changing the contents of this file
HADOOP_HOME=${HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
LDBC_SNB_DATAGEN_HOME=${LDBC_SNB_DATAGEN_HOME:-$DEFAULT_LDBC_SNB_DATAGEN_HOME}

# use a temporary file to allow overiding default parameter file
TEMP_PARAM_FILE=/tmp/graknParams.ini

export HADOOP_HOME
export LDBC_SNB_DATAGEN_HOME

# load the ontology to the SNB keyspace
REMOTE_PATH=`cat params.ini | grep grakn.engine.uri | sed "s/grakn.engine.uri://"`
graql.sh -k SNB -f $SCRIPTPATH/./graql/snb-ontology-simple.gql -r $REMOTE_PATH

# generate params.ini file that uses Grakn serializers
cat $LDBC_SNB_DATAGEN_HOME/params.ini | sed 's/small.CSV/grakn.Grakn/' > $TEMP_PARAM_FILE

# compile the data loader
mvn clean
mvn -DskipTests assembly:assembly

# hack (uses 7zip) because osx by default is not case sensitive
7z d -r $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-0.2.5-jar-with-dependencies.jar license

# increase memory
export HADOOP_CLIENT_OPTS="-Xmx1024m"

# execute loader
$HADOOP_HOME/bin/hadoop jar $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-0.2.5-jar-with-dependencies.jar $TEMP_PARAM_FILE

rm -f m*personFactors*
rm -f .m*personFactors*
rm -f m*activityFactors*
rm -f .m*activityFactors*
rm -f m0friendList*
rm -f .m0friendList*
