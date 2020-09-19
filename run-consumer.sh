#!/bin/bash

mvn -f kafka-consumer/pom.xml compile exec:java -Dexec.mainClass="com.cinlogic.Consumer"
