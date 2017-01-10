package com.ebuer.hbase;

import com.ebuer.conf.HbaseConf;
import com.ebuer.exception.HbaseComponentException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 原生的 hbase 的ddl & dml操作
 * ddl___________
 * 1、新建表
 * 2、删除表
 * 3、删除列族
 * 4、新增列族
 * 目前ddl只提供以上简单的4种通用方法；
 * 外部扩展example：
 * HbaseClient hbaseComponent = new HbaseClient();
 * HBaseAdmin admin = null;
 * try {
 *      admin = hbaseComponent.getAdmin();
 *      // do what you want to do
 * } finally {
 *      hbaseComponent.releaseAdmin(admin);
 * }
 *
 *
 * dml___________
 * 1、CRDU操作
 * 外部扩展example：
 * HbaseClient hbaseComponent = new HbaseClient();
 * HTable table = null;
 * try{
 *      table = hbaseComponet.getHTable("demo");
 *      //do what you want
 * }finaly{
 *      releaseTable(table);
 * }
 *
 *
 *
 * 使用:HbaseClient
 * 1、建议在 spring中注入 HbaseLocalConf对象
 * 2、如果你没有用spring 那么请用传入合适的HbaseLocalConf
 *
 * @author xu.qiang
 * @date 2016/12/30.
 */
public class HbaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HbaseClient.class);

    private HbaseConf hbaseConf;


    public HbaseClient(HbaseConf hbaseConf) {
        this.hbaseConf = hbaseConf;
    }

    /**
     * 获取Admin
     *
     * @return
     */
    public Admin getAdmin() throws IOException {
        Connection connection = ConnectionFactory.createConnection(hbaseConf.getHbaseConf());
        return connection.getAdmin();
    }

    /**
     * 释放资源
     *
     * @param admin
     */
    public void releaseAdmin(Admin admin) {
        try {
            if (admin != null) {
                admin.close();//内部会把connection也给关闭了的 不需要重复关闭connection
            }
        } catch (IOException e) {
            logger.error("HbaseClient releaseAdmin error :{}", e);
        }
    }


    /**
     * 获取HTable
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public Table getHTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(hbaseConf.getHbaseConf());

        return connection.getTable(TableName.valueOf(tableName));
    }

    /**
     * 释放资源
     *
     * @param table
     */
    public void releaseTable(Table table) {
        try {
            table.close();
            if (table != null) {
                table.close();//内部会把connection也给关闭了的 不需要重复关闭connection
            }
        } catch (IOException e) {
            logger.error("HbaseClient releaseTable error :{}", e);
        }
    }

    /*华丽的分割线——01  以下是通用的ddl方法*/

    /**
     * 创建表 带列族 内部不校验表是否存在(提供外部方法 让调用方校验表是否存在)
     *
     * @param tableName
     * @param columnFamily
     */
    public boolean createTable(final String tableName, final String... columnFamily) {
        try {
            return this.executeAdminAction(new AdminAction<Boolean>() {
                public Boolean excute(HBaseAdmin admin) throws IOException {

                    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

                    for (String family : columnFamily) {
                        tableDescriptor.addFamily(new HColumnDescriptor(family));
                    }

                    admin.createTable(tableDescriptor);
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error createTable.  Cause: " + e, e);
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public boolean dropTable(final String... tableName) {
        try {
            return this.executeAdminAction(new AdminAction<Boolean>() {
                public Boolean excute(HBaseAdmin admin) throws IOException {
                    for (String table : tableName) {
                        admin.disableTable(table);
                        admin.deleteTable(table);
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error dropTable.  Cause: " + e, e);
        }
    }

    /**
     * 判断是否存在该表
     *
     * @param tableName
     */
    public boolean existsTable(final String tableName) {
        try {
            return this.executeAdminAction(new AdminAction<Boolean>() {
                public Boolean excute(HBaseAdmin admin) throws IOException {
                    return admin.tableExists(tableName);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error existsTable.  Cause: " + e, e);
        }
    }


    /**
     * 查询hbase下表信息
     *
     * @param regx
     * @return
     */
    public TableName[] listTables(final String regx) {
        try {
            return this.executeAdminAction(new AdminAction<TableName[]>() {
                public TableName[] excute(HBaseAdmin admin) throws IOException {
                    if (StringUtils.isBlank(regx)) {
                        return admin.listTableNames();
                    } else {

                        return admin.listTableNames(regx);
                    }
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error listTabels.  Cause: " + e, e);
        }
    }

    /**
     * 新增列族
     *
     * @param tableName
     * @param familys
     * @return
     */
    public boolean addColumnFamily(final String tableName, final String... familys) {
        try {
            return this.executeAdminAction(new AdminAction<Boolean>() {
                public Boolean excute(HBaseAdmin admin) throws IOException {
                    for (String family : familys) {
                        admin.addColumn(tableName, new HColumnDescriptor(family));
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error addColumnFamily.  Cause: " + e, e);
        }
    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param familys
     * @return
     */
    public boolean deleteColumnFamily(final String tableName, final String... familys) {
        try {
            return this.executeAdminAction(new AdminAction<Boolean>() {
                public Boolean excute(HBaseAdmin admin) throws IOException {
                    for (String family : familys) {
                        admin.deleteColumn(tableName, family);
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error deleteColumnFamily.  Cause: " + e, e);
        }
    }

    /*华丽的分割线——02  以下是通用的dml方法*/


    /**
     * 往tableName表中插入一行or更新一行
     *
     * @param tableName
     * @param put
     * @return
     */
    public boolean putRow(String tableName, final Put put) {
        try {
            return this.executeTableAction(tableName, new TableAction<Boolean>() {
                public Boolean excute(Table table) throws IOException {
                    table.put(put);
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error putRow.  Cause: " + e, e);
        }
    }

    /**
     * 往tableName表中插入多行or更新 内部分解巨大的puts 分批上传
     *
     * @param tableName
     * @param puts
     * @return
     */
    public boolean putRows(String tableName, final List<Put> puts) {
        try {
            return this.executeTableAction(tableName, new TableAction<Boolean>() {
                public Boolean excute(Table table) throws IOException {
                    if (puts.size() <= 2048) {
                        table.put(puts);
                        return true;
                    }

                    List<Put> realPuts = new ArrayList<Put>(2048);
                    for (Put put : puts) {
                        realPuts.add(put);

                        if (realPuts.size() == 2048) {
                            table.put(realPuts);
                            realPuts = new ArrayList<Put>(2048);
                        }
                    }
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error putRows.  Cause: " + e, e);
        }

    }

    /**
     * 删除一行
     *
     * @param tableName
     * @param delete
     * @return
     */
    public boolean deleteRow(String tableName, final Delete delete) {
        try {
            return this.executeTableAction(tableName, new TableAction<Boolean>() {
                public Boolean excute(Table table) throws IOException {
                    table.delete(delete);
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error deleteRow.  Cause: " + e, e);
        }

    }

    /**
     * 删除一行
     *
     * @param tableName
     * @param deletes
     * @return
     */
    public boolean deleteRows(String tableName, final List<Delete> deletes) {
        try {
            return this.executeTableAction(tableName, new TableAction<Boolean>() {
                public Boolean excute(Table table) throws IOException {
                    table.delete(deletes);
                    return true;
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error deleteRows.  Cause: " + e, e);
        }
    }

    /**
     * 查询一行
     *
     * @param tableName
     * @param rowKey
     * @return
     */
    public Result getRow(String tableName, final String rowKey) {
        try {
            return this.executeTableAction(tableName, new TableAction<Result>() {
                public Result excute(Table table) throws IOException {
                    Get get = new Get(Bytes.toBytes(rowKey));
                    return table.get(get);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error getRow.  Cause: " + e, e);
        }
    }

    /**
     * 查询一行
     *
     * @param tableName
     * @param get
     * @return
     */
    public Result getRow(String tableName, final Get get) {
        try {
            return this.executeTableAction(tableName, new TableAction<Result>() {
                public Result excute(Table table) throws IOException {
                    return table.get(get);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error getRow.  Cause: " + e, e);
        }
    }

    /**
     * 获取多行
     *
     * @param tableName
     * @param gets
     * @return
     */
    public Result[] getRows(String tableName, final List<Get> gets) {
        try {
            return this.executeTableAction(tableName, new TableAction<Result[]>() {
                public Result[] excute(Table table) throws IOException {
                    return table.get(gets);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error getRows.  Cause: " + e, e);
        }
    }

    /**
     * 获取多行
     *
     * @param tableName
     * @param startRowkey
     * @param endRowkey
     * @return
     */
    public ResultScanner getRows(String tableName, final String startRowkey, final String endRowkey) {
        try {
            return this.executeTableAction(tableName, new TableAction<ResultScanner>() {
                public ResultScanner excute(Table table) throws IOException {
                    Scan scan = new Scan(Bytes.toBytes(startRowkey), Bytes.toBytes(endRowkey));
                    return table.getScanner(scan);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error getRows.  Cause: " + e, e);
        }
    }


    /**
     * 全表扫描tableName查询
     *
     * @param tableName
     * @param scan
     * @return
     */
    public ResultScanner getRows(String tableName, final Scan scan) {
        try {
            return this.executeTableAction(tableName, new TableAction<ResultScanner>() {
                public ResultScanner excute(Table table) throws IOException {
                    return table.getScanner(scan);
                }
            });
        } catch (IOException e) {
            throw new HbaseComponentException("Error getRows.  Cause: " + e, e);
        }
    }


    public interface AdminAction<T> {
        T excute(HBaseAdmin admin) throws IOException;
    }

    interface TableAction<T> {
        T excute(Table table) throws IOException;
    }


    /**
     * HAdminAction的通用执行器【内部如果发生IO异常 直接扔运行时异常 报警当前系统环境不稳定】
     * 内部 流程化 管理好资源，外部不用关心，只需要传入执行的Action即可
     *
     * @param action
     * @return
     */

    private <T> T executeAdminAction(AdminAction<T> action) throws IOException {
        Connection connection = null;
        HBaseAdmin admin = null;
        try {
            connection = ConnectionFactory.createConnection(hbaseConf.getHbaseConf());
            admin = (HBaseAdmin) connection.getAdmin();

            return action.excute(admin);
        } finally {
            releaseAdmin(admin);
        }
    }


    /**
     * HTableAction的通用执行器
     * 内部 流程化 管理好资源，外部不用关心，只需要传入对哪张表做了什么动作即可
     *
     * @param tableName
     * @param tableAction
     * @return
     */
    private <T> T executeTableAction(String tableName, TableAction<T> tableAction) throws IOException {

        Connection connection = null;
        Table table = null;

        try {
            connection = ConnectionFactory.createConnection(hbaseConf.getHbaseConf());
            table = connection.getTable(TableName.valueOf(tableName));
            return tableAction.excute(table);
        } finally {
            releaseTable(table);
        }
    }

}
