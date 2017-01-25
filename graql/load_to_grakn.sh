#!/bin/bash
$1/bin/graql.sh -f ldbc-snb-ontology.gql
time $1/bin/migration.sh csv -s \| -t template-user.gql -i test_data/social_network/user_0_0.csv
tail -n +2 test_data/social_network/user_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-message.gql -i test_data/social_network/message_0_0.csv
tail -n +2 test_data/social_network/message_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-tag.gql -i test_data/social_network/tag_0_0.csv
tail -n +2 test_data/social_network/tag_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-message_replyOf_message.gql -i test_data/social_network/message_replyOf_message_0_0.csv
tail -n +2 test_data/social_network/message_replyOf_message_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-message_tags_tag.gql -i test_data/social_network/message_tags_tag_0_0.csv
tail -n +2 test_data/social_network/message_tags_tag_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-user_knows_user.gql -i test_data/social_network/user_knows_user_0_0.csv
tail -n +2 test_data/social_network/user_knows_user_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-user_likes_message.gql -i test_data/social_network/user_likes_message_0_0.csv
tail -n +2 test_data/social_network/user_likes_message_0_0.csv | wc -l
time $1/bin/migration.sh csv -s \| -t template-user_writes_message.gql -i test_data/social_network/user_writes_message_0_0.csv
tail -n +2 test_data/social_network/user_writes_message_0_0.csv | wc -l
