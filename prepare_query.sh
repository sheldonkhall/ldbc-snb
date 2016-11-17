#!/bin/bash
tail -n +1 test_data/social_network/user_0_0.csv | awk 'BEGIN {FS="\|"} ; {print $1}' > match_query.csv
