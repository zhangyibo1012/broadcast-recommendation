package cn.orgtec.hix.broadcast.recommendation.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * HBaseConnection
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
public class HBaseConnection {

    private static final HBaseConnection INSTANCE = new HBaseConnection();

    private static Configuration configuration;

    private static Connection connection;

    private HBaseConnection() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                configuration.set("hbase.zookeeper.quorum", "hix2-virtual-machine");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 获取 HBase 链接
     *
     * @return Connection
     */
    public static Connection getHBaseConnection() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取 Table 实例
     *
     * @param tableName tableName
     * @return Table
     * @throws IOException IOException
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭 HBase 链接
     */
    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     *  获取 HBase 链接
     *
     * @return  Connection
     */
    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
