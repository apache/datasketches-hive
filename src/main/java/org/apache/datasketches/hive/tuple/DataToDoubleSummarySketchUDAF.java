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
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * This simple implementation is to give an example of a concrete UDAF based on the abstract
 * DataToSketchUDAF if no extra arguments are needed. The same functionality is included into
 * DataToDoubleSummaryWithModeSketchUDAF with the default summary mode of Sum, but the
 * implementation is more complex because of the extra argument.
 */

@Description(
  name = "DataToDoubleSummarySketch",
  value = "_FUNC_(key, double value, nominal number of entries, sampling probability)",
  extended = "Returns a Sketch<DoubleSummary> as a binary blob that can be operated on by other"
    + " tuple sketch related functions. The nominal number of entries is optional, must be a power"
    + " of 2 and controls the relative error expected from the sketch."
    + " A number of 16384 can be expected to yield errors of roughly +-1.5% in the estimation of"
    + " uniques. The default number is defined in the sketches-core library, and at the time of this"
    + " writing was 4096 (about 3% error)."
    + " The sampling probability is optional and must be from 0 to 1. The default is 1 (no sampling)")
public class DataToDoubleSummarySketchUDAF extends DataToSketchUDAF {

  @SuppressWarnings("resource")
  @Override
  public GenericUDAFEvaluator getEvaluator(final GenericUDAFParameterInfo info) throws SemanticException {
    super.getEvaluator(info);
    final ObjectInspector[] inspectors = info.getParameterObjectInspectors();
    ObjectInspectorValidator.validateGivenPrimitiveCategory(inspectors[1], 1, PrimitiveCategory.DOUBLE);
    return createEvaluator();
  }

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new DataToDoubleSummarySketchEvaluator();
  }

  static class DataToDoubleSummarySketchEvaluator extends DataToSketchEvaluator<Double, DoubleSummary> {

    private static final SummaryDeserializer<DoubleSummary> SUMMARY_DESERIALIZER =
        new DoubleSummaryDeserializer();
    private static final SummaryFactory<DoubleSummary> SUMMARY_FACTORY =
        new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
    private static final SummarySetOperations<DoubleSummary> SUMMARY_SET_OPS =
        new DoubleSummarySetOperations(DoubleSummary.Mode.Sum);

    @Override
    protected SummaryDeserializer<DoubleSummary> getSummaryDeserializer() {
      return SUMMARY_DESERIALIZER;
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactory(final Object[] data) {
      return SUMMARY_FACTORY;
    }

    @Override
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForIterate(final Object[] data) {
      return null; // not needed for building sketches
    }

    @Override
    protected SummarySetOperations<DoubleSummary> getSummarySetOperationsForMerge(final Object data) {
      return SUMMARY_SET_OPS;
    }

  }

}
