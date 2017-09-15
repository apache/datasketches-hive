/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

@Description(
    name = "ArrayOfDoublesSketchesTTest",
    value = "_FUNC_(sketchA, sketchB)",
    extended = "Performs t-test and returns a list of p-values given two ArrayOfDoublesSketches."
    + " The result will be N double values, where N is the number of double values kept in the"
    + " sketch per key. The resulting p-values are probabilities that differences in means are"
    + " due to chance")
public class ArrayOfDoublesSketchesTTestUDF extends UDF {

  /**
   * T-test on a given pair of ArrayOfDoublesSketches
   * @param serializedSketchA ArrayOfDoublesSketch in as serialized binary
   * @param serializedSketchB ArrayOfDoublesSketch in as serialized binary
   * @return list of p-values
   */
  public List<Double> evaluate(final BytesWritable serializedSketchA, final BytesWritable serializedSketchB) {
    if (serializedSketchA == null || serializedSketchB == null) { return null; }
    final ArrayOfDoublesSketch sketchA =
        ArrayOfDoublesSketches.wrapSketch(Memory.wrap(serializedSketchA.getBytes()));
    final ArrayOfDoublesSketch sketchB =
        ArrayOfDoublesSketches.wrapSketch(Memory.wrap(serializedSketchB.getBytes()));

    if (sketchA.getNumValues() != sketchB.getNumValues()) {
      throw new IllegalArgumentException("Both sketches must have the same number of values");
    }

    // If the sketches contain fewer than 2 values, the p-value can't be calculated
    if (sketchA.getRetainedEntries() < 2 || sketchB.getRetainedEntries() < 2) {
      return null;
    }

    final SummaryStatistics[] summariesA = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchA);
    final SummaryStatistics[] summariesB = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchB);

    final TTest tTest = new TTest();
    final List<Double> pValues = new ArrayList<Double>(sketchA.getNumValues());
    for (int i = 0; i < sketchA.getNumValues(); i++) {
      pValues.add(tTest.tTest(summariesA[i], summariesB[i]));
    }
    return pValues;
  }

}
