package com.ebuer.hbase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 利用Phoenix访问Hbase
 *
 * @author xu.qiang
 * @date 2016/12/30.
 */
public class PhoenixClientTest {

    /**
     * Just for phoenix test！
     */
    @Test
    public void testSelect() {
        PhoenixClient client = new PhoenixClient();
        client.setHost("dev01");
        client.setPort(2181);
        String pheonixSQL = " select * from \"p_table_01\" order by id desc";

        String result = client.execQuerySql(pheonixSQL);
        System.out.println(result);
    }


    @Test
    public void testUpsert() {
        PhoenixClient client = new PhoenixClient();
        client.setHost("dev01");
        client.setPort(2181);

        String rk = null;
        String name = null;
        String sex = null;

        for (int i = 1; i < 100000; i++) {

            rk = "rk_" + i;
            name = "xuqiang_" + i;
            sex = "man";
            String base = " upsert into \"p_table_01\" values(\'" + rk + "\',\'" + name + "\',\'" + sex + "\')";

            client.execSql(base);
        }
    }

    @Test
    public void testUpsertBatch() {
        PhoenixClient client = new PhoenixClient();
        client.setHost("dev01");
        client.setPort(2181);

        String rk = null;
        String name = null;
        String sex = null;

        List<String> list = new ArrayList<String>();
        for (int i = 1; i < 100000; i++) {

            rk = "rk_" + i;
            name = "xuqiang_" + i;
            sex = "man";
            String base = " upsert into \"p_table_01\" values(\'" + rk + "\',\'" + name + "\',\'" + sex + "\')";

            list.add(base);
        }


        client.execBatchSql(list);
    }

    @Test
    public void testDelete() {
        PhoenixClient client = new PhoenixClient();
        client.setHost("dev01");
        client.setPort(2181);

        client.execSql("delete from \"p_table_01\" ");

    }
}