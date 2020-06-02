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

import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.SketchIterator;
import org.apache.datasketches.tuple.Sketches;
import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

@Description(
    name = "DoubleSummarySketchToEstimates",
    value = "_FUNC_(sketch)",
    extended = "Returns a list of estimates from a given Sketch<DoubleSummary>."
    + " The result will be two double values."
    + " The first estimate is the estimate of the number of unique keys in the"
    + " original population. Next there is an estimate of the sum of the parameter in the"
    + " original population (sum of the values in the sketch scaled to the original population."
    + " This estimate assumes that the DoubleSummary was used in the Sum mode.)")
@SuppressWarnings("javadoc")
public class DoubleSummarySketchToEstimatesUDF extends UDF {

  private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
      new DoubleSummaryDeserializer();

  /**
   * Get estimates from a given Sketch&lt;DoubleSummary&gt;
   * @param serializedSketch DoubleSummarySketch in a serialized binary form
   * @return list of estimates
   */
  public List<Double> evaluate(final BytesWritable serializedSketch) {
    if (serializedSketch == null) { return null; }
    final Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(BytesWritableHelper.wrapAsMemory(serializedSketch), SUMMARY_DESERIALIZER);
    double sum = 0;
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      sum += it.getSummary().getValue();
    }
    return Arrays.asList(
      sketch.getEstimate(),
      sum / sketch.getTheta()
    );
  }

}
