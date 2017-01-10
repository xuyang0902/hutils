package com.ebuer.hbase;

import com.ebuer.conf.HbaseConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试Hbase的组件
 *
 * @author xu.qiang
 * @date 2017/1/9.
 */
public class HbaseClientTest {


    private HbaseClient hbaseClient = null;


    private Configuration conf = null;


    @Before
    public void init() {
        ArrayList<Pair<String, String>> keyValues = new ArrayList<Pair<String, String>>(1);
        Pair pair = new Pair("hbase.zookeeper.quorum", "192.168.48.133:2181");
        Pair pair2 = new Pair("hbase.zookeeper.property.clientPort", "2181");
        Pair pair3 = new Pair("hbase.master", "192.168.48.133:2181");
        keyValues.add(pair);
        keyValues.add(pair2);
        keyValues.add(pair3);
        HbaseConf conf = new HbaseConf(keyValues);
        hbaseClient = new HbaseClient(conf);
    }


    @Test
    public void testCreatTable() {

        System.out.println("开始创建表");

        hbaseClient.createTable("user", "base_info", "other");
    }


    @Test
    public void deleteTable() {

        System.out.println("开始删除 user");
        hbaseClient.dropTable("user");
    }

    @Test
    public void listTables() {

        System.out.println("查询表");
        TableName[] tableNames = hbaseClient.listTables("user");
        for (TableName tableName : tableNames) {
            System.out.println(Bytes.toString(tableName.getNamespace()) + "-->" + Bytes.toString(tableName.getName()));
        }
    }

    @Test
    public void createColumn(){
        hbaseClient.addColumnFamily("user","column_added");
    }

    @Test
    public void put(){
        Put put = new Put(Bytes.toBytes("rk_001"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("xuyang"));
        put.addColumn(Bytes.toBytes("other"),Bytes.toBytes("desc"),Bytes.toBytes("handsome"));
        hbaseClient.putRow("user",put);
    }


    @Test
    public void get(){
        Result row = hbaseClient.getRow("user", "rk_001");
        List<Cell> cells = row.listCells();
        for(Cell cell : cells){
            System.out.println(Bytes.toString(cell.getFamilyArray()));
        }
    }

}
