#!/usr/bin/env bash

set -e

cd ./data

wget  https://ikeptwalking.com/wp-content/uploads/2017/04/Employees50K.zip
wget  https://ikeptwalking.com/wp-content/uploads/2017/04/Employees100K.zip
curl -O https://download.elastic.co/demos/kibana/gettingstarted/7.x/shakespeare.json
curl -O https://download.elastic.co/demos/kibana/gettingstarted/7.x/accounts.zip
curl -O https://download.elastic.co/demos/kibana/gettingstarted/7.x/logs.jsonl.gz

cd ..
