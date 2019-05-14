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
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.codec.binary.Hex;

import java.sql.*;

import org.junit.Test;

public class FunctionTest {

    @Test
    public void testReverse() throws Exception {
        Connection conn = null;

        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "jdbc:phoenix:master,slave1,slave2:2181";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(url);
            } catch (SQLException e) {
                e.printStackTrace();
            }


            String ddl = "CREATE TABLE IF NOT EXISTS REVERSE_TEST (pk VARCHAR NOT NULL PRIMARY KEY)";
            conn.createStatement().execute(ddl);
            /* String dml = "UPSERT INTO REVERSE_TEST VALUES('abc')";*/
//        conn.createStatement().execute(dml);
            conn.commit();
            conn.close();

/*        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT reverse(pk) FROM REVERSE_TEST");
        assertTrue(rs.next());
        assertEquals("cba", rs.getString(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT pk FROM REVERSE_TEST WHERE pk=reverse('cba')");
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertFalse(rs.next());*/
        }


    }
}
