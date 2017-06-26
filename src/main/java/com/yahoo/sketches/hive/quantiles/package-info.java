/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

/**
 * Hive UDFs for Quantiles sketches.
 * This includes UDFs for generic ItemsSketch and specialized DoublesSketch.
 * 
 * <p>The generic implementation is in the form of abstract classes DataToItemsSketchUDAF and
 * UnionItemsSketchUDAF to be specialized for particular types of items.
 * An implementation for strings is provided: DataToStringsSketchUDAF, UnionStringsSketchUDAF,
 * plus UDFs to obtain the results from sketches:
 * GetQuantileFromStringsSketchUDF, GetQuantilesFromStringsSketchUDF and GetPmfFromStringsSketchUDF.
 * 
 * <p>Support for DoublesSketch: DataToDoublesSketchUDAF, UnionDoublesSketchUDAF,
 * GetQuantileFromDoublesSketchUDF, GetQuantilesFromDoublesSketchUDF, GetPmfFromDoublesSketchUDF
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.hive.quantiles;
