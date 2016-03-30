[![][travis img]][travis] [![][coveralls img]][coveralls]

=================

#Sketch UDFs for Hive 

Depends on sketches-core.

To use Hive UDFs, you should do the following:

1. Register the JAR file with Hive: 
  - `hive> add jar sketches-hive-X.Y.Z-incDeps.jar`
2. Register UDF functions with Hive:z
  - `hive> create temporary function estimate as 'com.yahoo.sketches.hive.theta.EstimateSketchUDF';`
  - `hive> create temporary function dataToSketch as 'com.yahoo.sketches.hive.theta.DataToSketchUDAF';`
3. Run a query: 
  - `hive> select estimate(dataToSketch(myCol, 16384, 1.0)) from myTable where color = blue;`

[travis]:https://travis-ci.org//DataSketches/sketches-hive/builds?branch=master
[travis img]:https://secure.travis-ci.org/DataSketches/sketches-hive.svg?branch=master

[coveralls]:https://coveralls.io/github/DataSketches/sketches-hive?branch=master
[coveralls img]:https://coveralls.io/repos/DataSketches/sketches-hive/badge.svg?branch=master
