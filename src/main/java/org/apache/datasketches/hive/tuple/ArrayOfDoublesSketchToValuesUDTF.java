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
import java.util.Arrays;
import java.util.List;

import org.apache.datasketches.hive.common.BytesWritableHelper;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

@SuppressWarnings("javadoc")
public class ArrayOfDoublesSketchToValuesUDTF extends GenericUDTF {

  PrimitiveObjectInspector inputObjectInspector;

  @SuppressWarnings("deprecation")
  @Override
  public StructObjectInspector initialize(final ObjectInspector[] inspectors) throws UDFArgumentException {
    if (inspectors.length != 1) {
      throw new UDFArgumentException("One argument expected");
    }
    if (inspectors[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Primitive argument expected, but "
          + inspectors[0].getCategory().name() + " was recieved");
    }
    inputObjectInspector = (PrimitiveObjectInspector) inspectors[0];
    if (inputObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.BINARY) {
      throw new UDFArgumentTypeException(0, "Binary value expected as the first argument, but "
          + inputObjectInspector.getPrimitiveCategory().name() + " was recieved");
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("values"),
      Arrays.asList(
        ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        )
      )
    );
  }

  @Override
  public void process(final Object[] data) throws HiveException {
    if (data == null || data[0] == null) { return; }
    final BytesWritable serializedSketch =
      (BytesWritable) inputObjectInspector.getPrimitiveWritableObject(data[0]);
    final ArrayOfDoublesSketch sketch = ArrayOfDoublesSketches.wrapSketch(
        BytesWritableHelper.wrapAsMemory(serializedSketch));
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      forward(new Object[] { primitivesToList(it.getValues()) });
    }
  }

  @Override
  public void close() throws HiveException {
  }

  static List<Double> primitivesToList(final double[] array) {
    final List<Double> result = new ArrayList<>(array.length);
    for (double item: array) { result.add(item); }
    return result;
  }

}
