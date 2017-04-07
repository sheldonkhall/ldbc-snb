#!/bin/bash

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`
DATA=$SCRIPTPATH/./social_network
GRAQL=$SCRIPTPATH/./graql

# generate CSV files
$SCRIPTPATH/run.sh

# load ontology
graql.sh -k $2 -f $GRAQL/ldbc-snb-1-resources.gql -r $1
graql.sh -k $2 -f $GRAQL/ldbc-snb-2-relations.gql -r $1
graql.sh -k $2 -f $GRAQL/ldbc-snb-3-entities.gql -r $1

# load entities
time migration.sh csv -s \| -t $GRAQL/template-person.gql -i $DATA/person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-comment.gql -i $DATA/comment_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/comment_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-forum.gql -i $DATA/forum_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/forum_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-organization.gql -i $DATA/organisation_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/organisation_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-place.gql -i $DATA/place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/place_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-post.gql -i $DATA/post_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/post_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-tag.gql -i $DATA/tag_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/tag_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-tagclass.gql -i $DATA/tagclass_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/tagclass_0_0.csv | wc -l

# load relationships
time migration.sh csv -s \| -t $GRAQL/template-comment_hasCreator_person.gql -i $DATA/comment_hasCreator_person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/comment_hasCreator_person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-comment_isLocatedIn_place.gql -i $DATA/comment_isLocatedIn_place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/comment_isLocatedIn_place_0_0.csv | wc -l

sed -i '' "1s/Comment/Message/" $DATA/comment_replyOf_comment_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-comment_replyOf_comment.gql -i $DATA/comment_replyOf_comment_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/comment_replyOf_comment_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-comment_replyOf_post.gql -i $DATA/comment_replyOf_post_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/comment_replyOf_post_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-forum_containerOf_post.gql -i $DATA/forum_containerOf_post_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/forum_containerOf_post_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-forum_hasMember_person.gql -i $DATA/forum_hasMember_person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/forum_hasMember_person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-forum_hasModerator_person.gql -i $DATA/forum_hasModerator_person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/forum_hasModerator_person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-forum_hasTag_tag.gql -i $DATA/forum_hasTag_tag_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/forum_hasTag_tag_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-organization_isLocatedIn_place.gql -i $DATA/organisation_isLocatedIn_place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/organisation_isLocatedIn_place_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_email_emailaddress.gql -i $DATA/person_email_emailaddress_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_email_emailaddress_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_hasInterest_tag.gql -i $DATA/person_hasInterest_tag_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_hasInterest_tag_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_isLocatedIn_place.gql -i $DATA/person_isLocatedIn_place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_isLocatedIn_place_0_0.csv | wc -l

sed -i '' "1s/Person/Person1/" $DATA/person_knows_person_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-person_knows_person.gql -i $DATA/person_knows_person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_knows_person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_likes_comment.gql -i $DATA/person_likes_comment_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_likes_comment_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_likes_post.gql -i $DATA/person_likes_post_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_likes_post_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_speaks_language.gql -i $DATA/person_speaks_language_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_speaks_language_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_studyAt_organisation.gql -i $DATA/person_studyAt_organisation_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_studyAt_organisation_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-person_workAt_organisation.gql -i $DATA/person_workAt_organisation_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/person_workAt_organisation_0_0.csv | wc -l

sed -i '' "1s/Place/Place1/" $DATA/place_isPartOf_place_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-place_isPartOf_place.gql -i $DATA/place_isPartOf_place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/place_isPartOf_place_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-post_hasCreator_person.gql -i $DATA/post_hasCreator_person_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/post_hasCreator_person_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-post_hasTag_tag.gql -i $DATA/post_hasTag_tag_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/post_hasTag_tag_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-post_isLocatedIn_place.gql -i $DATA/post_isLocatedIn_place_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/post_isLocatedIn_place_0_0.csv | wc -l

time migration.sh csv -s \| -t $GRAQL/template-tag_hasType_tagclass.gql -i $DATA/tag_hasType_tagclass_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/tag_hasType_tagclass_0_0.csv | wc -l

sed -i '' "1s/TagClass/TagClass1/" $DATA/tagclass_isSubclassOf_tagclass_0_0.csv
time migration.sh csv -s \| -t $GRAQL/template-tagclass_isSubclassOf_tagclass.gql -i $DATA/tagclass_isSubclassOf_tagclass_0_0.csv -k $2 -u $1 -a ${3:-25} -b ${4:-25}
tail -n +2 $DATA/tagclass_isSubclassOf_tagclass_0_0.csv | wc -l