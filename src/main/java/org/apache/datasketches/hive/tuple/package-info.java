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
 * <p>The generic implementation is in the form of abstract classes DataToSketchUDAF and
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
package org.apache.datasketches.hive.tuple;
