## LDBC Social Network Benchmark - a data generator for GRAKN.AI

This generator generates a gql file containg SNB data that can be loaded into a Grakn graph using Grakn Engine. You can find more information on SNB data [here](https://github.com/ldbc/ldbc_snb_datagen), and more about Grakn [here](https://grakn.ai/pages/index.html). 

## Loading a dataset

A small sample dataset (`ldbc-snb-data-person-100.gql`). is included.

To load this file into Grakn, you need to do the following:

0 . Download a Grakn distribution or clone and build Grakn from [https://github.com/graknlabs/grakn](https://github.com/graknlabs/grakn). Further instructions for either option can be found in the [Grakn setup guide](https://grakn.ai/pages/documentation/get-started/setup-guide.html).

1 . Make sure you have Grakn running:
```
cd [your Grakn install directory]
bin/grakn.sh start
```

2 . Use the following command to load the schema file:

``` curl -H "Content-Type: application/json" -X POST -d '{"path":"FILE_PATH/ldbc-snb-ontology.gql"}' http://localhost:4567/import/ontology ```

where FILE_PATH is the root folder of this project, which contains the Graql schema file `ldbc-snb-ontology.gql`.

3 . You can then load data using the following command:

``` curl -H "Content-Type: application/json" -X POST -d '{"path":"FILE_PATH/ldbc-snb-data-person-100.gql"}' http://localhost:4567/import/batch/data ```

again, FILE_PATH is the project root path. Loading this dataset can take up to 30 seconds.

4 . Start graql shell:

```
bin/graql.sh
```

5 . Check if the data has been loaded by using the following graql query

``` >>> compute count in person ```

``` 100 ```

You can visualise a Graql query by using the [Grakn visualiser](http://localhost:4567/#/shell). Make a query and Submit it to see the result.

## Generating your own dataset

If you want to generate your own dataset, you can customize it by modifying params.ini in project root folder.

You can set the number of person in your dataset by setting the following
``` ldbc.snb.datagen.generator.numPersons ```

Or you can simply set the scale factor
``` ldbc.snb.datagen.generator.scaleFactor ```

For more information on how to customize the dataset, please check [LDBC on Github](https://github.com/ldbc/ldbc_snb_datagen).

To generate the dataset, simply run the following command in the terminal:
``` FILE_PATH/run.sh ```

where FILE_PATH is the path of this project, containing the shell script.

Generating the data can take from several minutes to several hours,
depending on the size of the dataset.

When it's done, you can find the generated graql file in the project folder:

``` ldbc-snb-data.gql ```

Load the customized data to Grakn using the following command:

``` curl -H "Content-Type: application/json" -X POST -d '{"path":"FILE_PATH/ldbc-snb-data-person.gql"}' http://localhost:4567/import/batch/data ```

Note that you still need to load the schema file before loading your customized data!

For help in using Grakn, please visit our [discussion boards](https://discuss.grakn.ai) or post over on [Stack Overflow](http://stackoverflow.com/questions/tagged/graql). 
