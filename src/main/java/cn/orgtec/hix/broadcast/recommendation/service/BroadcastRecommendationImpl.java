package cn.orgtec.hix.broadcast.recommendation.service;

import cn.hutool.core.convert.Convert;
import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestComment;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestFavor;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseConstant;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * rowKey 格式: HBase 表唯一标识  发布者 Id _ 操作者 Id : 广播 ID
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Service
@Slf4j
public class BroadcastRecommendationImpl implements BroadcastRecommendation {

    @Override
    public Boolean saveHBaseBroadcast(HBaseBroadcast hBaseBroadcast) {
        Long broadcastId = hBaseBroadcast.getBroadcastId();
        Integer broadcastUserId = hBaseBroadcast.getBroadcastUserId();

        List<Integer> userIds = HBaseConstant.getUserIds();

        for (int i = 0; i < userIds.size(); i ++){
            if (!broadcastUserId.equals(userIds.get(i))){

//                rowKey  broadcastUserId_操作者id:broadcastId
                String rowKey = broadcastUserId + "_" + userIds.get(i) + ":" + broadcastId;

                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_SD, "broadcastId", Convert.toStr(broadcastId));

                // TODO: 2019/5/9
//                操作者 id
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_SD, "handlerUserId", Convert.toStr(userIds.get(i)));

                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_SD, "broadcastUserId", Convert.toStr(broadcastUserId));

                // TODO: 2019/5/9    广播权重100  80  60  40 20
//                动态数据 广播权重
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "broadcastWeight", Convert.toStr(50));

//                时间戳 time
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "time", Convert.toStr(System.currentTimeMillis()));

                // TODO: 2019/5/9   类型A  50   类型B  40  类型C  30  类型D  20
//                类型权重
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "typeWeight", Convert.toStr(100));

//                总亲密度 广播权重加类型权重
                HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, HBaseConstant.HBASE_BRO_CF_DD, "sumIntimacy", Convert.toStr(50 + 100));
            }
        }
        return true;
    }

    @Override
    public Boolean saveFavorHBaseIntimacy(RequestFavor requestFavor) {
        Long broadcastId = requestFavor.getBroadcastId();
        Integer broadcastUserId = requestFavor.getBroadcastUserId();
        Integer favorUserId = requestFavor.getFavorUserId();
//
        String rowKey = broadcastUserId + "_"  +favorUserId + ":" +  broadcastId;
//
        log.info("rowKey : {}", rowKey);

//        根据 rowKey 查询亲密度
        Result hBaseIntimacy = HBaseUtil.getRow(HBaseConstant.HBASE_BRO_TABLE, rowKey);

//        获取亲密度
        String intimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes("dd"), Bytes.toBytes("intimacy"))
        );

//        获取总亲密度
        String sumIntimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes("dd"), Bytes.toBytes("sumIntimacy")));

        if(null == intimacy){
//            在 dd 列族新增一个属性  默认点赞 5
            HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, "dd", "intimacy", "5");

//            删除总亲密度这一列
            HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey,"dd" , "sumIntimacy");

            Integer updateSumIntimacy = Convert.toInt(sumIntimacy) + 5;

//            更新总亲密度
            HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, "dd", "sumIntimacy", Convert.toStr(updateSumIntimacy));
        } else {
            HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey,"dd" , "sumIntimacy");
            HBaseUtil.deleteQualifier(HBaseConstant.HBASE_BRO_TABLE, rowKey,"dd" , "intimacy");

            Integer updateIntimacy = Convert.toInt(intimacy) + 5;
            Integer updateSumIntimacy = Convert.toInt(sumIntimacy) + 5;

            HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, "dd", "intimacy", Convert.toStr(updateIntimacy));
            HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, "dd", "updateSumIntimacy", Convert.toStr(updateSumIntimacy));
        }
        return true;
    }

    @Override
    public Boolean saveCommentHBaseIntimacy(RequestComment requestComment) {
        Long broadcastId = requestComment.getBroadcastId();
        Integer broadcastUserId = requestComment.getBroadcastUserId();
        Integer commentUserId = requestComment.getCommentUserId();

        String rowKey = broadcastUserId + "_"  +commentUserId + ":" +  broadcastId;

        log.info("rowKey : {}", rowKey);

//        根据 rowKey 查询亲密度
        Result hBaseIntimacy = HBaseUtil.getRow(HBaseConstant.HBASE_BRO_TABLE, rowKey);

//        获取亲密度
        String intimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes("dd"), Bytes.toBytes("intimacy"))
        );

//        获取总亲密度
        String sumIntimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes("dd"), Bytes.toBytes("sumIntimacy")));

        if(null == intimacy){
//            在 dd 列族新增一个属性  默认点赞 5
            HBaseUtil.putRow(HBaseConstant.HBASE_BRO_TABLE, rowKey, "dd", "intimacy", "8");

//            删除总亲密度这一列
            HBaseUtil.deleteQualifier("hix_bro_recommend", rowKey,"dd" , "sumIntimacy");

            Integer updateSumIntimacy = Convert.toInt(sumIntimacy) + 8;

//            更新总亲密度
            HBaseUtil.putRow("hix_bro_recommend", rowKey, "dd", "sumIntimacy", Convert.toStr(updateSumIntimacy));
        }
        return true;
    }
}
