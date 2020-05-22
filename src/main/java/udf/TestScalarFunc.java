package udf;

import org.apache.flink.table.functions.ScalarFunction;


public class TestScalarFunc extends ScalarFunction {

    private int factor = 2020;

    public int eval() {
        return factor;
    }

    public int eval(int a) {
        return a * factor;
    }

    public int eval(int... a) {
        int res = 1;
        for (int i : a) {
            res *= i;
        }
        return res * factor;
    }
}
