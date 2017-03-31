#!/bin/bash

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`
DATA=$SCRIPTPATH/./social_network
GRAQL=$SCRIPTPATH/./graql

# generate CSV files
$SCRIPTPATH/run.sh

# load ontology
graql.sh -k $2 -f $GRAQL/snb-ontology-simple.gql -r $1

# load data
time migration.sh csv -s \| -t $GRAQL/template-user.gql -i $DATA/user_0_0.csv -k $2 -u $1
tail -n +2 $DATA/user_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-message.gql -i $DATA/message_0_0.csv -k $2 -u $1
tail -n +2 $DATA/message_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-tag.gql -i $DATA/tag_0_0.csv -k $2 -u $1
tail -n +2 $DATA/tag_0_0.csv | wc -l

sed -i '' "1s/Message/Comment/" $DATA/message_replyOf_message_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-message_replyOf_message.gql -i $DATA/message_replyOf_message_0_0.csv -k $2 -u $1
tail -n +2 $DATA/message_replyOf_message_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-message_tags_tag.gql -i $DATA/message_tags_tag_0_0.csv -k $2 -u $1
tail -n +2 $DATA/message_tags_tag_0_0.csv | wc -l

sed -i '' "1s/User/User1/" $DATA/user_knows_user_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-user_knows_user.gql -i $DATA/user_knows_user_0_0.csv -k $2 -u $1
tail -n +2 $DATA/user_knows_user_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-user_likes_message.gql -i $DATA/user_likes_message_0_0.csv -k $2 -u $1
tail -n +2 $DATA/user_likes_message_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-user_writes_message.gql -i $DATA/user_writes_message_0_0.csv -k $2 -u $1
tail -n +2 $DATA/user_writes_message_0_0.csv | wc -l
