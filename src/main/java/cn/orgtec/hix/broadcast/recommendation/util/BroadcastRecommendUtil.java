package cn.orgtec.hix.broadcast.recommendation.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 广播推荐工具类
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@UtilityClass
public class BroadcastRecommendUtil {

    /**
     *  按照时间戳获取最新的广播
     *
     * @param userId  用户 id
     * @return  ResultScanner
     */
    public ResultScanner getNewBroId(Integer userId) throws IOException {

//        定义过滤器规则 通过 表名 列族 属性值 比较
        List<Filter> filters = new ArrayList<>();

//        列族 列明 属性值 当前操作用户的 id  查询出来不包含该用户的广播
        filters.add(new SingleColumnValueFilter(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD),
                Bytes.toBytes("handlerUserId"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes(userId + "") ));

        filters.add( new SingleColumnValueFilter(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD),
                Bytes.toBytes("broadcastUserId"),
                CompareFilter.CompareOp.NOT_EQUAL,Bytes.toBytes(userId + "")));

        filters.add(  new SingleColumnValueFilter(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD),
                Bytes.toBytes("time"),
                CompareFilter.CompareOp.NOT_EQUAL,Bytes.toBytes("null")));

        FilterList queryFilter= new FilterList(filters);

        Scan scan =new Scan();

        scan.setFilter(queryFilter);

//            查询 时间戳 和 广播 id rowKey
        scan.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("handlerUserId"));
        scan.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("broadcastUserId"));
        scan.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("time"));
        scan.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("broadcastId"));

//          获得 表 实例
        Table table = HBaseConnection.getTable(HBaseConstant.HBASE_BRO_TABLE);

//         执行查询
        return  table.getScanner(scan);
    }

    /**
     * 更新 HBase 的时间戳为 null
     *
     * @param broRowKeyMap       rowKey 和广播 id 的 map
     * @param recommendBroadcastIds     推荐的广播 id 集合
     * @param i                         index
     */
    public void updateTimestamp(Map<Long, String> broRowKeyMap, List<Long> recommendBroadcastIds, int i) {


        String rowKey = broRowKeyMap.get(recommendBroadcastIds.get(i));

//     据 rowKey 删除属性列 time
        HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, Convert.toStr(rowKey), HBaseConstant.HBASE_BRO_CF_DD, "time");

//     更新 time 为 null
        HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, Convert.toStr(rowKey), HBaseConstant.HBASE_BRO_CF_DD, "time", "null");
    }

    /**
     *  根据用户id 和亲密度倒序查询并更新亲密度
     *
     * @param userId  用户 id
     * @return        推荐的广播 id 集合
     */
    public List<Long> getBroIdsByIntimacy(Integer userId) throws IOException {

        Table table = HBaseConnection.getTable(HBaseConstant.HBASE_BRO_TABLE);
        Scan s =new Scan();
        List<Filter> filters = new ArrayList<>();

        filters.add(  new SingleColumnValueFilter(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD),
                Bytes.toBytes("handlerUserId"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes(userId + "") ));
        FilterList queryFilter= new FilterList(filters);

        s.setFilter(queryFilter);

//            查询 广播权重 总亲密 广播id rowKey
        s.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("broadcastWeight"));
        s.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("sumIntimacy"));
        s.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("broadcastId"));
        s.addColumn(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("handlerUserId"));
        ResultScanner rs = table.getScanner(s);
//           广播 Id  亲密度
        Map<Long, Long> broIdIntimacy = MapUtil.newHashMap();

//            广播Id  广播权重
        Map<Long, Long> broIdAndWeight = MapUtil.newHashMap();

//            广播 id 和 rowKey
        Map<Long, String> broIdAndRowKey = MapUtil.newHashMap();

        for (Result r : rs) {

            byte[] broadcastWeight = r.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("broadcastWeight"));
            byte[] sumIntimacy = r.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("sumIntimacy"));
            byte[] broId = r.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("broadcastId"));
            broIdIntimacy.put(Convert.toLong(new String(broId)),Convert.toLong(new String(sumIntimacy)));
            broIdAndWeight.put(Convert.toLong(new String(broId)), Convert.toLong(new String(broadcastWeight)));
            broIdAndRowKey.put(Convert.toLong(new String(broId)), Convert.toStr(new String(r.getRow())));

            System.out.println("广播权重:" + new String(broadcastWeight) + "总亲密度:" + new String(sumIntimacy) + "广播 id :" +  new String(broId) +  "主键rowKey :" + new String(r.getRow()));
        }


//            根据 map value 亲密度倒序
        Map<Long ,Long > result = new LinkedHashMap<>();
        broIdIntimacy.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue()
                        .reversed()).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        System.out.println("=================亲密度倒序排列===============================");
        List<Long> recommendBroadcastIds = new ArrayList<>();
        result.forEach((k , v ) -> {
            recommendBroadcastIds.add(k);
            if (recommendBroadcastIds.size() <= 10){

//                   根据广播 id 获取 rowKey  广播权重
                String rowKey = broIdAndRowKey.get(k);
                Long broadcastWeight = broIdAndWeight.get(k);
                System.out.println("广播 id: " + k + ",broadcastWeight:" + broadcastWeight + "总亲密度：" + v + "rowKey = " + rowKey);
//                    更新列族下的广播权重属性值
                HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey + "", HBaseConstant.HBASE_BRO_CF_DD, "broadcastWeight");
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey + "", HBaseConstant.HBASE_BRO_CF_DD,"broadcastWeight" , (broadcastWeight - 5 ) + "");

//                    更新总亲密度
                HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey + "", HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy");
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey + "", HBaseConstant.HBASE_BRO_CF_DD,"sumIntimacy" , ((v) - 5 ) + "");
            }
        });
        return recommendBroadcastIds;
    }

    public static void main(String[] args) {
        Map<Long, Long> intimacyBroId = MapUtil.newHashMap();
        intimacyBroId.put(1L, 10L);
        intimacyBroId.put(12L, 8L);
        intimacyBroId.put(5L, 100L);


//        根据 value 倒序
        Map<Long ,Long > descValueresult = new LinkedHashMap<>();
        intimacyBroId.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue()
                        .reversed()).forEachOrdered(e -> descValueresult.put(e.getKey(), e.getValue()));

        descValueresult.forEach((k , v ) -> System.out.println("k = " + k  + "v" + v));

        System.out.println("==============================================" );

//        根据 key 倒序
        Map<Long ,Long > descKeyresult = new LinkedHashMap<>();
        intimacyBroId.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByKey()
                        .reversed()).forEachOrdered(e -> descKeyresult.put(e.getKey(), e.getValue()));
        descKeyresult.forEach((k , v ) -> System.out.println("k = " + k  + "v" + v));

        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(1);
        list.add(2);
        list.add(2);
        list.add(3);
        System.out.println("list = " + list);
        ArrayList<Integer> distinct = CollUtil.distinct(list);
        System.out.println("distinct = " + distinct);
    }
}
