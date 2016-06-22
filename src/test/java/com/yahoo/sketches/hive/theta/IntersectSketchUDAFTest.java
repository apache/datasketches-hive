package com.yahoo.sketches.hive.theta;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

public class IntersectSketchUDAFTest {

  static final ObjectInspector binaryInspector =
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.BINARY);

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooFewInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentException.class)
  public void getEvaluatorTooManyInspectors() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector, binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongCategory() throws Exception {
    ObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("a"),
      Arrays.asList(binaryInspector)
    );
    ObjectInspector[] inspectors = new ObjectInspector[] { structInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test(expectedExceptions = UDFArgumentTypeException.class)
  public void getEvaluatorWrongType() throws Exception {
    ObjectInspector intInspector =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT);
    ObjectInspector[] inspectors = new ObjectInspector[] { intInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    new IntersectSketchUDAF().getEvaluator(info);
  }

  @Test
  public void iterateTerminatePartial() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL1, inspectors);
    checkResultInspector(resultInspector);

    IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
        (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(3);
    state.update(sketch1.toByteArray());

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    sketch2.update(3);
    sketch2.update(4);

    eval.iterate(state, new Object[] { new BytesWritable(sketch2.toByteArray()) });

    BytesWritable bytes = (BytesWritable) eval.terminatePartial(state);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getRetainedEntries(true), 2);
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    eval.close();
  }

  @Test
  public void mergeTerminate() throws Exception {
    ObjectInspector[] inspectors = new ObjectInspector[] { binaryInspector };
    GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(inspectors, false, false);
    GenericUDAFEvaluator eval = new IntersectSketchUDAF().getEvaluator(info);
    ObjectInspector resultInspector = eval.init(Mode.PARTIAL2, inspectors);
    checkResultInspector(resultInspector);

    IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState state =
        (IntersectSketchUDAF.IntersectSketchUDAFEvaluator.IntersectionState) eval.getNewAggregationBuffer();

    UpdateSketch sketch1 = UpdateSketch.builder().build();
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(3);
    state.update(sketch1.toByteArray());

    UpdateSketch sketch2 = UpdateSketch.builder().build();
    sketch2.update(2);
    sketch2.update(3);
    sketch2.update(4);

    eval.merge(state, new BytesWritable(sketch2.toByteArray()));

    BytesWritable bytes = (BytesWritable) eval.terminate(state);
    Sketch resultSketch = Sketches.heapifySketch(new NativeMemory(bytes.getBytes()));
    Assert.assertEquals(resultSketch.getRetainedEntries(true), 2);
    Assert.assertEquals(resultSketch.getEstimate(), 2.0);
    eval.close();
  }

  private static void checkResultInspector(ObjectInspector resultInspector) {
    Assert.assertNotNull(resultInspector);
    Assert.assertEquals(resultInspector.getCategory(), ObjectInspector.Category.PRIMITIVE);
    Assert.assertEquals(
      ((PrimitiveObjectInspector) resultInspector).getPrimitiveCategory(),
      PrimitiveObjectInspector.PrimitiveCategory.BINARY
    );
  }

}
