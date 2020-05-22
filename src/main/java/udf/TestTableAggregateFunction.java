package udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TestTableAggregateFunction extends TableAggregateFunction<Row,TestTableAggregateFunction.Top2> {
    @Override
    public Top2 createAccumulator() {
        Top2 t = new Top2();
        t.f1 = Integer.MIN_VALUE;
        t.f2 = Integer.MIN_VALUE;

        return t;
    }

    public void accumulate(Top2 t, Integer v) {
        if (v > t.f1) {
            t.f2 = t.f1;
            t.f1 = v;
        } else if (v > t.f2) {
            t.f2 = v;
        }
    }

    public void merge(Top2 t, Iterable<Top2> iterable) {
        for (Top2 otherT : iterable) {
            accumulate(t, otherT.f1);
            accumulate(t, otherT.f2);
        }
    }

    public void emitValue(Top2 t, Collector<Row> out) {
        Row row = null;
        if (t.f1 != Integer.MIN_VALUE) {
            row = new Row(2);
            row.setField(0,t.f1);
            row.setField(1,1);
            out.collect(row);
        }
        if (t.f2 != Integer.MIN_VALUE) {
            row = new Row(2);
            row.setField(0,t.f2);
            row.setField(1,2);
            out.collect(row);
        }
    }

    public void emitUpdateWithRetract(Top2 t, RetractableCollector<Row> out) {
        Row row = null;
        if (!t.f1.equals(t.oldF1)) {
            // if there is an update, retract old value then emit new value.
            if (t.oldF1 != Integer.MIN_VALUE) {
                row = new Row(2);
                row.setField(0,t.oldF1);
                row.setField(1,1);
                out.retract(row);
            }
            row = new Row(2);
            row.setField(0,t.f1);
            row.setField(1,1);
            out.collect(row);
            t.oldF1 = t.f1;
        }

        if (!t.f2.equals(t.oldF2)) {
            // if there is an update, retract old value then emit new value.
            if (t.oldF2 != Integer.MIN_VALUE) {
                row = new Row(2);
                row.setField(0,t.oldF2);
                row.setField(1,2);
                out.retract(row);
            }
            row = new Row(2);
            row.setField(0,t.f2);
            row.setField(1,2);
            out.collect(row);
            t.oldF2 = t.f2;
        }
    }

    public class Top2{
        public Integer f1;
        public Integer f2;
        public Integer oldF1;
        public Integer oldF2;

    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.INT,Types.INT);
    }
}
