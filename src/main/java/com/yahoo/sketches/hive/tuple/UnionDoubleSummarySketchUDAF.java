/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hive.tuple;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import com.yahoo.sketches.tuple.DoubleSummary;
import com.yahoo.sketches.tuple.DoubleSummaryFactory;
import com.yahoo.sketches.tuple.SummaryFactory;

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
public class UnionDoubleSummarySketchUDAF extends UnionSketchUDAF {

  @Override
  public GenericUDAFEvaluator createEvaluator() {
    return new UnionDoubleSummarySketchEvaluator();
  }

  public static class UnionDoubleSummarySketchEvaluator extends UnionSketchEvaluator<DoubleSummary> {

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactoryForIterate(final Object[] data) {
      return new DoubleSummaryFactory();
    }

    @Override
    protected SummaryFactory<DoubleSummary> getSummaryFactoryForMerge(final Object data) {
      return new DoubleSummaryFactory();
    }

  }

}
