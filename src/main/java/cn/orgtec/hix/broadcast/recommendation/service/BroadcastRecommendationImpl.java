package cn.orgtec.hix.broadcast.recommendation.service;

import cn.hutool.core.convert.Convert;
import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseConstant;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseUtil;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Service
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
    public Boolean saveFavorHBaseIntimacy(HBaseBroadcast hBaseBroadcast) {
        Long broadcastId = hBaseBroadcast.getBroadcastId();
        Integer broadcastUserId = hBaseBroadcast.getBroadcastUserId();

        String rowKey = broadcastUserId + "" + behaviorEntity.getUserId() + behaviorEntity.getBroadcastId();

        log.info("rowKey : {}", rowKey);

        return true;
    }
}
