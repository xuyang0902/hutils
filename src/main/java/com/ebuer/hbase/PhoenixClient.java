package com.ebuer.hbase;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ebuer.exception.PhoenixException;
import org.apache.phoenix.jdbc.PhoenixResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 利用Phoenix访问Hbase
 *
 * @author xu.qiang
 * @date 2016/12/30.
 */
public class PhoenixClient {

    /**
     * zookeeper的master-host
     */
    private String host;

    /**
     * zookeeper的master-port
     */
    private int port;

    // Phoenix DB不支持直接设置连接超时 所以这里使用线程池的方式来控制数据库连接超时
    private static ThreadPoolExecutor threadPool = null;

    /**
     * 利用静态块的方式初始化Driver 以及线程池
     */
    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

            threadPool = new ThreadPoolExecutor(4,
                    Runtime.getRuntime().availableProcessors(),
                    10L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingDeque<Runnable>(10),
                    new RejectedExecutionHandler() {
                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });

            threadPool.allowCoreThreadTimeOut(true);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取一个Hbase-Phoenix的连接
     *
     * @param host zookeeper的master-host
     * @param port zookeeper的master-port
     * @return
     */
    private Connection getConnection(String host, int port) {
        Connection cc = null;
        final String url = "jdbc:phoenix:" + host + ":" + port;

        if (cc == null) {
            try {
                Callable<Connection> call = new Callable<Connection>() {
                    public Connection call() throws Exception {
                        return DriverManager.getConnection(url);
                    }
                };
                Future<Connection> future = threadPool.submit(call);
                // 如果在30s钟之内，还没得到 Connection 对象，则认为连接超时，不继续阻塞，防止服务夯死
                cc = future.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        return cc;
    }

    /**
     * 根据sql查询hbase中的内容;根据phoenix支持的SQL格式，查询Hbase的数据，并返回json格式的数据
     *
     * @param phoenixSQL sql语句
     * @return
     */
    public String execQuerySql(String phoenixSQL) {

        Connection conn = null;
        Statement stmt = null;
        try {
            // 获取一个Phoenix DB连接
            conn = this.getConnection(host, port);
            if (conn == null) {
                throw new PhoenixException("phoenix execSql connect time out");
            }

            // 准备查询
            stmt = conn.createStatement();
            PhoenixResultSet set = (PhoenixResultSet) stmt.executeQuery(phoenixSQL);

            // 查询出来的列是不固定的，所以这里通过遍历的方式获取列名
            ResultSetMetaData meta = set.getMetaData();
            ArrayList<String> cols = new ArrayList<String>();

            // 把最终数据都转成JSON返回
            JSONArray jsonArr = new JSONArray();
            while (set.next()) {
                if (cols.size() == 0) {
                    for (int i = 1, count = meta.getColumnCount(); i <= count; i++) {
                        cols.add(meta.getColumnName(i));
                    }
                }

                JSONObject json = new JSONObject();
                for (int i = 0, len = cols.size(); i < len; i++) {
                    json.put(cols.get(i), set.getString(cols.get(i)));
                }
                jsonArr.add(json);
            }
            // 结果封装
            JSONObject data = new JSONObject();
            data.put("data", jsonArr);
            return data.toString();
        } catch (SQLException e) {
            throw new PhoenixException("phoenix querySQl error:{}", e);
        } finally {
            release(conn, stmt);
        }
    }

    /**
     * 批量执行 内部每1024个sql提交一个批次 最后一次全部提交
     *
     * @param sqlList
     */
    public void execBatchSql(List<String> sqlList) {
        Connection conn = null;
        Statement stmt = null;
        try {
            // 获取一个Phoenix DB连接
            conn = this.getConnection(host, port);
            if (conn == null) {
                throw new PhoenixException("phoenix execSql connect time out");
            }

            Statement statement = conn.createStatement();
            int size = sqlList.size();
            for (int index = 0; index < size; index++) {

                statement.addBatch(sqlList.get(index));
                if (index == 1024) {
                    statement.executeBatch();
                }
            }

            statement.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new PhoenixException("phoenix querySQl error:{}", e);
        } finally {
            release(conn, stmt);
        }
    }

    /**
     * 执行其他sql
     *
     * @param sqls
     */
    public void execSql(String... sqls) {

        Connection conn = null;
        Statement stmt = null;
        try {
            // 获取一个Phoenix DB连接
            conn = this.getConnection(host, port);
            if (conn == null) {
                throw new PhoenixException("phoenix execSql connect time out");
            }

            for (String sql : sqls) {
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.execute();
            }

            conn.commit();
        } catch (SQLException e) {
            throw new PhoenixException("phoenix querySQl error:{}", e);
        } finally {
            release(conn, stmt);
        }
    }

    /**
     * 释放资源
     *
     * @param conn
     * @param stmt
     */
    private void release(Connection conn, Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}