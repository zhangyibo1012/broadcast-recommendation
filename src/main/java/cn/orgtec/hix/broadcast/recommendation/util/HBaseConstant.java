package cn.orgtec.hix.broadcast.recommendation.util;

import cn.hutool.core.convert.Convert;

import java.util.ArrayList;
import java.util.List;


/**
 * HBase 数据表
 *
 * @author Yibo Zhang
 * @date 2019/05/09
 */
public interface HBaseConstant {

    /**
     * HBase 表名
     */
    String HBASE_BRO_TABLE = "hix-broadcast-recommendation";


    /**
     * staticData  sd  静态数据列族
     * 广播 id 发布者 id 操作者 id
     */
    String HBASE_BRO_CF_SD = "sd";

    /**
     * dynamicData dd 动态数据
     * 广播权重 亲密度 总亲密度 时间戳 类型权重
     */
    String HBASE_BRO_CF_DD = "dd";


    /**
     * 第一次查询亲密度不等于 null 时的更新操作
     *
     * @param rowKey          主键
     * @param intimacy        亲密度
     * @param sumIntimacy     总亲密度
     * @param dynamicIntimacy 不同操作的对应着不同的亲密度
     */
    static void updateSumIntimacy(String rowKey, String intimacy, String sumIntimacy, Integer dynamicIntimacy) {
        HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy");
        HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "intimacy");

        Integer updateIntimacy = Convert.toInt(intimacy) + dynamicIntimacy;
        Integer updateSumIntimacy = Convert.toInt(sumIntimacy) + dynamicIntimacy;

        HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "intimacy", Convert.toStr(updateIntimacy));
        HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy", Convert.toStr(updateSumIntimacy));
    }

    /**
     * 保存亲密度
     *
     * @param rowKey      主键
     * @param sumIntimacy
     * @param dynamicIntimacy
     */
    static void saveSumIntimacy(String rowKey ,Integer sumIntimacy ,Integer dynamicIntimacy){
  //            在 dd 列族新增一个属性  默认打赏增加 10
        HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "intimacy", dynamicIntimacy + "");

//            删除总亲密度这一列
        HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy");

        Integer updateSumIntimacy = Convert.toInt(sumIntimacy) + dynamicIntimacy;

//            更新总亲密度
        HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy", Convert.toStr(updateSumIntimacy));
    }

    /**
     * 模拟获取用户 id
     *
     * @return
     */
    static List<Integer> getUserIds() {
        List<Integer> userId = new ArrayList<>();
        userId.add(60000);
        userId.add(60001);
        userId.add(60002);
        userId.add(60003);
        userId.add(60004);
        userId.add(60005);
        userId.add(60006);
        userId.add(60007);
        userId.add(60008);
        userId.add(60009);
        return userId;
    }


}
