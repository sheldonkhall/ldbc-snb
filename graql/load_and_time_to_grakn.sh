#!/bin/bash
$1/bin/grakn.sh start
$1/bin/graql.sh -f ldbc-snb-ontology.gql
time $1/bin/migration.sh csv -s \| -t template-message.gql -i test_data/social_network/message_0_0.csv
time $1/bin/migration.sh csv -s \| -t template-message_replyOf_message.gql -i test_data/social_network/message_replyOf_message_0_0.csv
$1/bin/grakn.sh stop
$1/bin/grakn.sh clean
