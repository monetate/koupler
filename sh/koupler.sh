#!/bin/bash

BASEDIR=$(dirname $0)

CLASSPATH=
for i in `ls build/libs/*.jar`
do
  CLASSPATH=${CLASSPATH}:${i}
done

java -Dlog4j.configurationFile=conf/log4j2.xml -cp ".:${CLASSPATH}" com.monetate.koupler.Koupler $@
