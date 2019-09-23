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
package org.apache.datasketches.hive.quantiles;
