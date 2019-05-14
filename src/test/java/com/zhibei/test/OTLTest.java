package com.zhibei.test;

import com.zhibei.otldb.api.EncryptApi;
import org.junit.Test;

public class OTLTest {

    @Test
    public void testEncrypt2() {

        String str = "张算少";
        System.out.println(EncryptApi.encrypt2(str));
    }

}
