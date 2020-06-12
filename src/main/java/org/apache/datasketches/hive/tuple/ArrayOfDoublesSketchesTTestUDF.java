/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.hive.tuple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "ArrayOfDoublesSketchesTTest",
    value = "_FUNC_(sketchA, sketchB)",
    extended = "Performs t-test and returns a list of p-values given two ArrayOfDoublesSketches."
    + " The result will be N double values, where N is the number of double values kept in the"
    + " sketch per key. The resulting p-values are probabilities that differences in means are"
    + " due to chance")
@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchesTTestUDF extends UDF {

  /**
   * T-test on a given pair of ArrayOfDoublesSketches
   * @param serializedSketchA ArrayOfDoublesSketch in as serialized binary
   * @param serializedSketchB ArrayOfDoublesSketch in as serialized binary
   * @return list of p-values
   */
  public List<Double> evaluate(final BytesWritable serializedSketchA, final BytesWritable serializedSketchB) {
    if ((serializedSketchA == null) || (serializedSketchB == null)) { return null; }
    final ArrayOfDoublesSketch sketchA =
        ArrayOfDoublesSketches.wrapSketch(BytesWritableHelper.wrapAsMemory(serializedSketchA));
    final ArrayOfDoublesSketch sketchB =
        ArrayOfDoublesSketches.wrapSketch(BytesWritableHelper.wrapAsMemory(serializedSketchB));

    if (sketchA.getNumValues() != sketchB.getNumValues()) {
      throw new IllegalArgumentException("Both sketches must have the same number of values");
    }

    // If the sketches contain fewer than 2 values, the p-value can't be calculated
    if ((sketchA.getRetainedEntries() < 2) || (sketchB.getRetainedEntries() < 2)) {
      return null;
    }

    final SummaryStatistics[] summariesA = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchA);
    final SummaryStatistics[] summariesB = ArrayOfDoublesSketchStats.sketchToSummaryStatistics(sketchB);

    final TTest tTest = new TTest();
    final List<Double> pValues = new ArrayList<>(sketchA.getNumValues());
    for (int i = 0; i < sketchA.getNumValues(); i++) {
      pValues.add(tTest.tTest(summariesA[i], summariesB[i]));
    }
    return pValues;
  }

}
