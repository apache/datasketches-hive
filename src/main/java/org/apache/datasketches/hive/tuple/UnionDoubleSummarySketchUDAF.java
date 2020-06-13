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

import org.apache.datasketches.tuple.SummaryDeserializer;
import org.apache.datasketches.tuple.SummaryFactory;
import org.apache.datasketches.tuple.SummarySetOperations;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * This simple implementation is to give an example of a concrete UDAF based on the abstract
 * UnionSketchUDAF if no extra arguments are needed.. The same functionality is included into
 * UnionDoubleSummaryWithModeSketchUDAF with the default summary mode of Sum, but the
 * implementation is more complex because of the extra argument.
 */

@Description(
  name = "UnionSketch",
  value = "_FUNC_(sketch, nominal number of entries)",
  extended = "Returns a Sketch<DoubleSummary> as a binary blob that can be operated on by other"
    + " tuple sketch related functions. The nominal number of entries is optional, must be a power of 2,"
    + " does not have to match the input sketches, and controls the relative error expected"
    + " from the sketch. A number of 16384 can be expected to yield errors of roughly +-1.5% in"
    + " the estimation of uniques. The default number is defined in the sketches-core library"
    + " and at the time of this writing was 4096 (about 3% error).")
@SuppressWarnings("javadoc")
public class UnionDoubleSummarySketchUDAF extends UnionSketchUDAF {

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new UnionDoubleSummarySketchEvaluator();
  }

  public static class UnionDoubleSummarySketchEvaluator extends UnionSketchEvaluator<DoubleSummary> {

    private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
        new DoubleSummaryDeserializer();
    private static final SummarySetOperations<DoubleSummary> SUMMARY_SET_OPS =
        new DoubleSummarySetOperations(DoubleSummary.Mode.Sum);

    @Override
    protected SummaryDeserializer<DoubleSummary> getSummaryDeserializer() {
      return SUMMARY_DESERIALIZER;
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactory(final Object[] data) {
      return null; // union never needs to create new instances
    }

    @Override
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForIterate(final Object[] data) {
      return SUMMARY_SET_OPS;
    }

    @Override
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForMerge(final Object data) {
      return SUMMARY_SET_OPS;
    }

  }

}
