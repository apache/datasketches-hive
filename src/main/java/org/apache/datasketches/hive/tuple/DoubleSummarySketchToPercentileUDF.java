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

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
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
    name = "DoubleSummarySketchToPercentile",
    value = "_FUNC_(sketch)",
    extended = "Returns a percentile from a given Sketch<DoubleSummary>."
  + " The values from DoubleSummary objects in the sketch are extracted,  and"
  + " a single value with a given normalized rank is returned. The rank is in"
  + " percent. For example, 50th percentile is the median value of the"
  + " distribution (the number separating the higher half of a probability"
  + " distribution from the lower half)")
@SuppressWarnings("javadoc")
public class DoubleSummarySketchToPercentileUDF extends UDF {

  private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
      new DoubleSummaryDeserializer();
  private static final int QUANTILES_SKETCH_K = 1024;

  /**
   * Get percentile from a given Sketch&lt;DoubleSummary&gt;
   * @param serializedSketch DoubleSummarySketch in as serialized binary
   * @param percentile normalized rank in percent
   * @return percentile value
   */
  public Double evaluate(final BytesWritable serializedSketch, final double percentile) {
    if (serializedSketch == null) { return null; }
    if ((percentile < 0) || (percentile > 100)) {
      throw new IllegalArgumentException("percentile must be between 0 and 100");
    }
    final Sketch<DoubleSummary> sketch =
        Sketches.heapifySketch(BytesWritableHelper.wrapAsMemory(serializedSketch), SUMMARY_DESERIALIZER);
    final UpdateDoublesSketch qs = DoublesSketch.builder().setK(QUANTILES_SKETCH_K).build();
    final SketchIterator<DoubleSummary> it = sketch.iterator();
    while (it.next()) {
      qs.update(it.getSummary().getValue());
    }
    return qs.getQuantile(percentile / 100);
  }

}
