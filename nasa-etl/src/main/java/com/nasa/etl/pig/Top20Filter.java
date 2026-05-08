package com.nasa.etl.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.*;

/**
 * Pig UDF: Top20Filter
 *
 * Accepts a bag of (resource_path, request_count, total_bytes, distinct_host_count)
 * tuples and returns only the top 20 by request_count descending.
 *
 * This is the Pig-side equivalent of the in-reducer top-20 logic in
 * {@link com.nasa.etl.mapreduce.Query2TopResources.TopResourceReducer}.
 *
 * Input  : bag of tuples
 *          (resource_path:chararray, request_count:long,
 *           total_bytes:long, distinct_host_count:long)
 * Output : bag with at most 20 tuples, same schema, sorted desc by request_count
 */
public class Top20Filter extends EvalFunc<DataBag> {

    private static final TupleFactory TF = TupleFactory.getInstance();
    private static final BagFactory   BF = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return BF.newDefaultBag();
        }

        DataBag inBag = (DataBag) input.get(0);

        // Collect all rows and sort by request_count desc
        List<Tuple> rows = new ArrayList<>();
        for (Tuple t : inBag) {
            if (t != null && t.size() >= 4) rows.add(t);
        }

        rows.sort((a, b) -> {
            try {
                long countA = toLong(a.get(1));
                long countB = toLong(b.get(1));
                return Long.compare(countB, countA); // descending
            } catch (Exception e) {
                return 0;
            }
        });

        int limit = Math.min(20, rows.size());
        DataBag outBag = BF.newDefaultBag();
        for (int i = 0; i < limit; i++) {
            outBag.add(rows.get(i));
        }
        return outBag;
    }

    private static long toLong(Object o) {
        if (o instanceof Number) return ((Number) o).longValue();
        try { return Long.parseLong(o.toString()); }
        catch (Exception e) { return 0L; }
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema rowSchema = new Schema();
            rowSchema.add(new Schema.FieldSchema("resource_path",       DataType.CHARARRAY));
            rowSchema.add(new Schema.FieldSchema("request_count",       DataType.LONG));
            rowSchema.add(new Schema.FieldSchema("total_bytes",         DataType.LONG));
            rowSchema.add(new Schema.FieldSchema("distinct_host_count", DataType.LONG));
            return new Schema(new Schema.FieldSchema("top20",
                    new Schema(new Schema.FieldSchema("t", rowSchema, DataType.TUPLE)),
                    DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }
}