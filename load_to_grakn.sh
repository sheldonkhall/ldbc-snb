#!/bin/bash
graql.sh -f ldbc-snb-ontology.gql
migration.sh csv -s \| -t template-user.gql -i test_data/social_network/user_0_0.csv
migration.sh csv -s \| -t template-message.gql -i test_data/social_network/message_0_0.csv