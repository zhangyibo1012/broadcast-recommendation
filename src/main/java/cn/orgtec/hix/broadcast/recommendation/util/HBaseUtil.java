package cn.orgtec.hix.broadcast.recommendation.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/04/30
 */
@Slf4j
public class HBaseUtil {

    /**
     *  创建命名空间
     *
     *   try (HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHBaseConnection().getAdmin())
     *   自动关闭资源
     *
     * @param namespace   命名空间
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {

        Admin admin =HBaseConnection.getHBaseConnection().getAdmin();

//        创建 ns 描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

//        创建操作
        admin.createNamespace(namespaceDescriptor);

//        关闭资源
        admin.close();
        HBaseConnection.closeConnection();
    }


    /**
     * 创建 HBase 表
     *
     * @param tableName 表明
     * @param cfs       列族的数组
     * @return boolean
     */
    public static boolean createTable(String tableName, String[] cfs) throws IOException {

        HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHBaseConnection().getAdmin();
        try {
            boolean tableExists = admin.tableExists(tableName);
            if (tableExists) {
                log.info("{}:表已经存在." ,tableName );
                return false;
            }

//            创建
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            System.out.println(tableDescriptor);
            Arrays.asList(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(10);
                tableDescriptor.addFamily(columnDescriptor);
            });

            admin.createTable(tableDescriptor);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            admin.close();
            HBaseConnection.closeConnection();
        }
        return true;
    }

    /**
     * 删除 hbase 表.
     *
     * @param tableName 表名
     * @return 是否删除成功
     */
    public static boolean deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHBaseConnection().getAdmin()) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * HBase 插入一条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @param cfName    列族名
     * @param qualifier 列标识
     * @param data      数据
     * @return 是否插入成功
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier,
                                 String data) {
        try (Table table = HBaseConnection.getTable(tableName)) {
//            put 数据模型
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return true;
    }

    /**
     * 批量插入数据
     *
     * @param tableName 表名
     * @param puts      List<Put> puts HBase 插入数据需要 Put 数据模型
     * @return 是否插入成功
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            table.put(puts);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return true;
    }

    /**
     * 获取单条数据.
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @return 查询结果
     */
    public static Result getRow(String tableName, String rowKey) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 使用过滤器条件获取数据
     *
     * @param tableName  表名
     * @param rowKey     唯一标识
     * @param filterList FilterList
     * @return 查询结果
     */
    public static Result getRow(String tableName, String rowKey, FilterList filterList) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
//            指定过滤器
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * Scan 检索数据 全表扫描
     *
     * @param tableName 表名
     * @return ResultScanner
     */
    public static ResultScanner getScanner(String tableName) {
        try (Table table = HBaseConnection.getTable(tableName)) {
//            Scan 实例
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索数据. 区间扫描
     *
     * @param tableName   表名
     * @param startRowKey 起始RowKey  包含头
     * @param endRowKey   终止RowKey  不包含尾
     * @return ResultScanner实例
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }finally {
            HBaseConnection.closeConnection();
        }
        return null;
    }

    /**
     * 批量过滤检索数据. 区间扫描.
     *
     * @param tableName   表名
     * @param startRowKey 起始RowKey
     * @param endRowKey   终止RowKey
     * @param filterList  FilterList
     * @return ResultScanner实例
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
                                           FilterList filterList) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     *  根据列族和属性查询
     * @param tableName
     * @param family
     * @param qualifier
     * @return
     */
//    public static ResultScanner getScanner(String tableName, String family, String qualifier ) {
//        try (Table table = HBaseConnection.getTable(tableName)) {
//            Scan scan = new Scan();
//            scan.setStartRow(Bytes.toBytes(startRowKey));
//            scan.setStopRow(Bytes.toBytes(endRowKey));
//            scan.setFilter(filterList);
//            scan.setCaching(1000);
//            return table.getScanner(scan);
//        } catch (IOException ioe) {
//            ioe.printStackTrace();
//        }
//        return null;
//    }

    /**
     * HBase删除一行记录.
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @return 是否删除成功
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return true;
    }

    /**
     * 删除某一个列族  对表结构进行更改
     *
     * @param tableName 表名
     * @param cfName    列族
     * @return 是否删除成功
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try (HBaseAdmin admin = (HBaseAdmin) HBaseConnection.getHBaseConnection().getAdmin()) {
            admin.deleteColumn(tableName, cfName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除 Qualifier
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @param cfName    列族
     * @param qualifier 属性
     * @return 是否删除成功
     */
    public static boolean deleteQualifier(String tableName, String rowKey, String cfName,
                                          String qualifier) {
        try (Table table = HBaseConnection.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return true;
    }
}