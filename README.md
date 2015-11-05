Sketch Adaptors for Hive [![Build Status](https://travis-ci.org/DataSketches/sketches-hive.svg?branch=master)](https://travis-ci.org/DataSketches/sketches-hive) [![Coverage Status](https://coveralls.io/repos/DataSketches/sketches-hive/badge.svg?branch=master)](https://coveralls.io/r/DataSketches/sketches-hive?branch=master)
=================
Depends on sketches-core.

To use Hive UDFs, you should do the following:

1. Register the JAR file with Hive: 
  - `hive> add jar sketches-hive-0.1.0-incDeps.jar`
2. Register UDF functions with Hive:
  - `hive> create temporary function estimate as 'com.yahoo.sketches.hive.theta.EstimateSketchUDF';`
  - `hive> create temporary function dataToSketch as 'com.yahoo.sketches.hive.theta.DataToSketchUDAF';`
3. Run a query: 
  - `hive> select estimate(dataToSketch(myCol, 16384, 1.0)) from myTable where color = blue;`
