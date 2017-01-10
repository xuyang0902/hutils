package com.ebuer.conf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

/**
 * Hbase的本地配置
 * <p>
 * 实际上客户端连接hbase只需要和zk沟通就好了。。
 *
 * @author xu.qiang
 * @date 2017/1/6.
 */
public class HbaseConf {


    /**
     * key-value: example
     * "hbase.zookeeper.quorum" --> "192.168.48.133:2182";
     * "hbase.zookeeper.property.clientPort" --> 2182
     * ""hbase.master"-->"192.168.48.133:2181"
     */
    private List<Pair<String, String>> keyValues;


    public HbaseConf() {
    }

    public HbaseConf(List<Pair<String, String>> keyValues) {
        this.keyValues = keyValues;
    }

    /**
     * 获取Hbase配置信息  添加个性化配置
     *
     * @return
     */
    public Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();

        if (!CollectionUtils.isEmpty(keyValues)) {
            for (Pair<String, String> pair : keyValues) {
                conf.set(pair.getFirst(), pair.getSecond());
            }
        }

        return conf;
    }


}
