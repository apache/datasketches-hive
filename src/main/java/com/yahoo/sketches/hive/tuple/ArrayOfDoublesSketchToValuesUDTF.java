package com.yahoo.sketches.hive.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketchIterator;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;

public class ArrayOfDoublesSketchToValuesUDTF extends GenericUDTF {

  PrimitiveObjectInspector inputObjectInspector;

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
        new NativeMemory(serializedSketch.getBytes()));
    final ArrayOfDoublesSketchIterator it = sketch.iterator();
    while (it.next()) {
      forward(new Object[] { primitivesToList(it.getValues()) });
    }
  }

  @Override
  public void close() throws HiveException {
  }

  static List<Double> primitivesToList(final double[] array) {
    final List<Double> result = new ArrayList<Double>(array.length);
    for (double item: array) { result.add(item); }
    return result;
  }

}
