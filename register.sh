#!/usr/bin/env bash

set -e

APP_NAME="scpload.$SCP_TENANT"
APP_TYPE=service
REDIR_URL="https://localhost/"
LOGIN_URL="https://localhost/"

echo "Register $APP_TYPE app : $APP_NAME"

if [ -z "$SCP_UID" ]
then
	echo -n "SCP username: "
	read username
	SCP_UID=$username
fi

if [ -z "$SCP_PWD" ]
then
	echo -n "SCP password: "
	read -s password
	echo 
	SCP_PWD=$password
fi

if [ -z "$SCP_TENANT" ]
then
	echo -n "SCP tenant: "
	read tenant
	SCP_TENANT=$tenant
fi

echo "logging in..."
scloud -u $SCP_UID -p $SCP_PWD login > /dev/null

if scloud -tenant $SCP_TENANT appreg get-app $APP_NAME > /dev/null 2>&1 ; 
then
    echo "delete existing app registration..." 
    scloud -tenant $SCP_TENANT appreg delete-app $APP_NAME > /dev/null
fi

echo "creating app registration..." 
scloud -tenant $SCP_TENANT appreg create-app $APP_NAME $APP_TYPE \
-redirect-urls $REDIR_URL \
-login-url $LOGIN_URL \
-title "$APP_NAME" \
-description "$APP_NAME" > appreg.json

echo "subscribing app..." 
scloud -tenant $SCP_TENANT appreg create-subscription $APP_NAME

echo "DONE"
