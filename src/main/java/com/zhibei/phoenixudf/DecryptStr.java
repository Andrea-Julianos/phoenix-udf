package com.zhibei.phoenixudf;

import com.zhibei.otldb.api.EncryptApi;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.shaded.org.apache.directory.api.util.Strings;
import org.apache.phoenix.util.StringUtil;

import java.sql.SQLException;
import java.util.List;


@FunctionParseNode.BuiltInFunction(name= DecryptStr.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class})} )
public class DecryptStr extends ScalarFunction {

    //必须是 大写
    public static final String NAME = "ENCRYPTSTR";

    public DecryptStr() {
    }

    public DecryptStr(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int targetOffset = ptr.getLength();
        if (targetOffset == 0) {
            return true;
        }

        byte[] strBytes = ptr.get();
        SortOrder sortOrder = arg.getSortOrder();


        int utf8Length = StringUtil.calculateUTF8Length(strBytes, 0, targetOffset, sortOrder);
        String utf8Str = Strings.utf8ToString(strBytes, utf8Length);

        String encryptStr = EncryptApi.encrypt2(utf8Str);
        byte[] bytesUtf8 = Strings.getBytesUtf8(encryptStr);

        ptr.set(bytesUtf8);







        /*成功案例*/
/*        String solvingStr = new String(ptr.copyBytes());
        String encryptStr = EncryptApi.encrypt2(solvingStr);

        ptr.set(PVarchar.INSTANCE.toBytes(encryptStr));*/



       /*
        String str = String.valueOf(source);
        String encryptStr = EncryptApi.encrypt2(str);
        target = encryptStr.getBytes();*/

       /*
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);
            targetOffset -= nBytes;
            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);
            sourceOffset += nBytes;
        }*/






        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {

        //指定该类的常量NAME
        return NAME;
    }
}
