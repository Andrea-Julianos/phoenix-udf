package com.zhibei.phoenixudf;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.DateScalarFunction;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.CurrentDateParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.joda.time.DateTime;

import java.sql.SQLException;
import java.util.List;

/*
 * @Description: 季节自定义函数，根据一个日期类型日期，返回一个字符串类型季节
 * @Auther: gucp
 * @Date: 2019/1/2 16:11
 *
 */
@FunctionParseNode.BuiltInFunction(
        name = "QUARTER",
        args = {@FunctionParseNode.Argument(
                allowedTypes = {PTimestamp.class}
        )}
)
public class DfQuarter extends DateScalarFunction {
    public static final String NAME = "QUARTER";

    public DfQuarter() {
    }

    public DfQuarter(List<Expression> children) throws SQLException {
        super(children);
    }

    public boolean evaluate(Tuple tuple, org.apache.hadoop.hbase.io.ImmutableBytesWritable immutableBytesWritable) {
        Expression expression = this.getChildren().get(0);
        if (!expression.evaluate(tuple, immutableBytesWritable)) {
            return false;
        }
        if (immutableBytesWritable.getLength() == 0) {
            return true;
        }

        long dateTime = this.inputCodec.decodeLong(immutableBytesWritable, expression.getSortOrder());
        DateTime dt = new DateTime(dateTime);
        int month = dt.getMonthOfYear();
        PDataType returnType = this.getDataType();


        byte[] byteValue = (dt.getYear() + "第" + getQuarterByMonth(month) + "季度").getBytes();


        immutableBytesWritable.set(byteValue);
        return true;


    }

    private String getQuarterByMonth(int month) {
        String quarter = "";
        if (month <= 3) {
            quarter = "1";
        } else if (month <= 6) {
            quarter = "2";
        } else if (month <= 9) {
            quarter = "3";
        } else {
            quarter = "4";
        }

        return quarter;
    }


    @Override
    public String getName() {
        return "QUARTER";
    }

    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }


}
