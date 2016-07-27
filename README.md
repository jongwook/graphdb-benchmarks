graphdb-benchmarks
==================

This is a fork of SocialSensor's [graphdb-benchmarks](https://github.com/socialsensor/graphdb-benchmarks) repo which adds the tests for [S2Graph](https://github.com/apache/incubator-s2graph).

## Setup

To test S2Graph, this project requires S2Graph JARs to be present in the local maven repository, typically at `~/.m2`. This is a temporary measure until an official release of S2Graph is released to the Maven Central. To install the JARs, run the following shell SBT command in the `incubator-s2graph` project's local directory.

    $ sbt publishM2

## Running

Provided that s2graph is installed locally at `~/.m2`, the `run.sh` script will run the benchmark:

    $ ./run.sh

The benchmark can also be launched by running the testcase `eu.socialsensor.main.GraphDatabaseBenchmarkTest`, or

    $ mvn -Pbench test

## Configuration

All configurations for the benchmark, including which graph/storage backend to use, need to be specified in the file `/test/resources/META-INF/input.properties`.

Additionally, it is recommended to give the JVM enough heap space, e.g.

    $ export MAVEN_OPTS="-Xmx16g"

## The Storage

The tests utilizes the local directory `/storage` for storing graph data representations, written by each backend. Keep in mind that the tests are stateful, and some may fail or produce different results according to the contents of the storage directory.

To reset the storage, run

    $ rm -r storage/*

## Caveat

When using HBase storage backend, an HBase server along with ZooKeeper will be launched in the same JVM, provided that the configured zookeeper host/port is not accessible. HBase server may fail in restricted networks like a public WiFi network; in those case, turn off the problematic network interface.

## The Data

To ease the benchmark process, this repository contains the data that were referenced by SocialSensor's original repository, at the directory `/data`.

