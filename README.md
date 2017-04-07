
![LDBC_LOGO](https://raw.github.com/wiki/ldbc/ldbc_socialnet_bm/images/ldbc-logo.png)
LDBC-SNB Data Generator
----------------------

The LDBC-SNB Data Generator (DATAGEN) is the responsible of providing the data sets used by all the LDBC benchmarks. This data generator is designed to produce directed labeled graphs that mimic the characteristics of those graphs of real data. A detailed description of the schema produced by datagen, as well as the format of the output files, can be found in the latest version of official [LDBC SNB specification document](https://github.com/ldbc/ldbc_snb_docs)


ldbc_snb_datagen is part of the LDBC project (http://www.ldbc.eu/).
ldbc_snb_datagen is GPLv3 licensed, to see detailed information about this license read the LICENSE.txt.

* **[Releases](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/releases)**
* **[Configuration](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Configuration)**
* **[Compilation and Execution](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Compilation_Execution)**
* **[Advanced Configuration](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Advanced_Configuration)**
* **[Output](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Data-Output)**
* **[Troubleshooting](https://github.com/ldbc-dev/ldbc_snb_datagen_0.2/wiki/Throubleshooting)**

**Datasets**

Publicly available datasets can be found at the LDBC-SNB Amazon Bucket. These datasets are the official SNB datasets and were generated using version 0.2.6. They are available in the three official supported serializers: CSV, CSVMergeForeign and TTL. The bucket is configured in "Requester Pays" mode, thus in order to access them you need a properly set up AWS client.
* http://ldbc-snb.s3.amazonaws.com/

**Community provided tools**

* **[Apache Flink Loader:] (https://github.com/s1ck/ldbc-flink-import)** A loader of LDBC datasets for Apache Flink

**Grakn test setup**

In order to run the SNB generator you need these pre-requisites:

* 7zip,
* Grakn,

and the `PATH` environmental variable should contain the `bin` directory of your Grakn distribution.
Finally, start the Grakn engine and the SNB data for the small graph can be loaded by executing either of the Grakn loading scripts:

`./runGraknREST.sh`

`./runGraknMigrator.sh localhost:4567 SNB`

`./runGraknGraql.sh`

**Grakn REST loader**

This script runs the snb data generator with serialisers that send the insert queries directly to the Grakn engine REST API.
If you need to load to a remote engine instance you can use these parameters in the params.ini file:

* grakn.engine.uri
* grakn.engine.keyspace

**Grakn Migrator loader**

This script runs the snb generator to create CSV files. These CSV files are then imported using the migrator.
The migrator script takes two arguments: the address of the engine instance and the keyspace to load that data in.

There are two optional arguments to the runGraknMigration.sh script to increase the system load:

`./runGraknMigrator.sh localhost:4567 SNB numberActiveTasks batchSize`

These two options are passed directly to the migration client by the script.

**Grakn Graql loader**

This script runs the snb data generator with serialisers that execute match and insert queries directly.
You can use a different keyspace by change the parameter in the params.ini file:

* grakn.engine.keyspace

NB: this script is only capable of loading data from a single machine.
