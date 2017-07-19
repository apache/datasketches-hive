/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */
/**
 * Hive UDFs for Tuple sketches.
 * Tuple sketches are based on the idea of Theta sketches with the addition of
 * values associated with unique keys.
 * Two sets of tuple sketch classes are available at the moment:
 * generic Tuple sketches with user-defined Summary, and a faster specialized
 * implementation with an array of double values.
 *
 * <p>There are two sets of Hive UDFs: one for generic Tuple sketch with an example
 * implementation for DoubleSummay, and another one for a specialized ArrayOfDoublesSketch.
 * 
 * <p> The generic implementation is in the form of abstract classes DataToSketchUDAF and
 * UnionSketchUDAF to be specialized for particular types of Summary.
 * An example implementation for DoubleSumamry is provided: DataToDoubleSummarySketchUDAF and
 * UnionDoubleSummarySketchUDAF, as well as UDFs to obtain the results from sketches:
 * DoubleSumamrySketchToEstimatesUDF and DoubleSummarySketchToPercentileUDF.
 * 
 * <p>UDFs for ArrayOfDoublesSketch: DataToArrayOfDoublesSketchUDAF, UnionArrayOfDoublesSketchUDAF,
 * ArrayOfDoublesSketchToEstimatesUDF, ArrayOfDoublesSketchToValuesUDTF.
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.hive.tuple;
