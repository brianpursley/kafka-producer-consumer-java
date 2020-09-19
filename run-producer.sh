#!/bin/bash

mvn -f kafka-producer/pom.xml compile exec:java -Dexec.mainClass="com.cinlogic.Producer"
