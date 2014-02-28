#!/bin/bash

FLUME_HOME="/opt/flume"

MAIN_ARTIFACT="target/apache-flume-jambalaya-0.8.0-SNAPSHOT.jar"

EXTERNAL_LIBS="target/lib/*.jar"

STAGING_FOLDER="/$FLUME_HOME/plugins.d/apache-flume-jambalaya"

LIB_FOLDER="$STAGING_FOLDER/lib"

LIBEXT_FOLDER="$STAGING_FOLDER/libext"

NATIVE_FOLDER="$STAGING_FOLDER/native"

rm -fR $STAGING_FOLDER

mkdir -p $LIB_FOLDER

mkdir -p $LIBEXT_FOLDER

mkdir -p $NATIVE_FOLDER

cp -p $EXTERNAL_LIBS $LIBEXT_FOLDER

cp -p $MAIN_ARTIFACT $LIB_FOLDER
