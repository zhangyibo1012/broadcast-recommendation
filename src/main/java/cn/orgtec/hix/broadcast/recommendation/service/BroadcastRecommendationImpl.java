package cn.orgtec.hix.broadcast.recommendation.service;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestComment;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestFavor;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestGiftReward;
import cn.orgtec.hix.broadcast.recommendation.util.BroadcastRecommendUtil;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseConstant;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

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
    public List<Long> getRecommendedBroadcastIds(Integer userId) {

        List<Long> broadcastIds = new ArrayList<>();
        try {
//            获取最新的广播 id
            ResultScanner newBroId = BroadcastRecommendUtil.getNewBroId(userId);

 //             时间戳  广播 Id
            Map<Long, Long> broTimeMap = MapUtil.newHashMap();

//            广播Id  主键
            Map<Long, String> broRowKeyMap = MapUtil.newHashMap();

            for (Result r : newBroId) {

//                主键 rowKey
                log.info("主键 rowKey :{}" ,new String(r.getRow()));

                byte[] time = r.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("time"));
                byte[] broId = r.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_SD), Bytes.toBytes("broadcastId"));

                log.info("时间戳==广播id==主键 :" + new String(time) + "==" + new String(broId) + "rowKey " + new String(r.getRow()));

                broTimeMap.put(Convert.toLong(new String(time)), Convert.toLong(new String(broId)));
                broRowKeyMap.put(Convert.toLong(new String(broId)), Convert.toStr(new String(r.getRow())));
            }

            Map<Long ,Long > descKeyResult = new LinkedHashMap<>();
            broTimeMap.entrySet().stream()
                    .sorted(Map.Entry.<Long, Long>comparingByKey()
                            .reversed()).forEachOrdered(e -> broTimeMap.put(e.getKey(), e.getValue()));

            List<Long> recommendBroadcastIds = new ArrayList<>();

            descKeyResult.forEach((k , bid ) -> recommendBroadcastIds.add(bid));

            log.info("recommendBroadcastIds.size() : {}" ,recommendBroadcastIds.size());
            if (!CollUtil.isEmpty(recommendBroadcastIds)){
   //                新广播大于 10 直接推荐新广播
                if (recommendBroadcastIds.size() >= 10){
                    for (int i = 0; i < 10 ; i ++) {
//                        广播 Id
                        log.info("推荐的新广播 id ：{}" , recommendBroadcastIds.get(i));
                        broadcastIds.add(recommendBroadcastIds.get(i));
                        BroadcastRecommendUtil.updateTimestamp(broRowKeyMap, broadcastIds, i);
                    }
                    return broadcastIds;
                } else {
 //                   新广播不够 10 推荐新广播 和 旧广播
                    for (int i = 0 ; i < recommendBroadcastIds.size(); i ++){
                        broadcastIds.add(recommendBroadcastIds.get(i));
                        BroadcastRecommendUtil.updateTimestamp(broRowKeyMap, broadcastIds, i);
                    }
//                    HBase 里面广播数量不够 11 的时候 HBaseUtil.deleteQualifier 会报空指针
                    List<Long> idsByIntimacy = BroadcastRecommendUtil.getBroIdsByIntimacy(userId);
                    int size = broadcastIds.size();

                    for (int i = 0 ; i < 10 - size ; i ++){
                        if ( ! broadcastIds.contains(idsByIntimacy.get(i))) {
                            broadcastIds.add(idsByIntimacy.get(i));
                        }
                    }
                    return broadcastIds;
                }
            } else {
 //                推荐老广播
                List<Long> idsByIntimacy = BroadcastRecommendUtil.getBroIdsByIntimacy(userId);
                System.out.println("idsByIntimacy.size = " + idsByIntimacy.size());
                for (int i = 0 ; i < 10  ; i ++){
                  if ( ! broadcastIds.contains(idsByIntimacy.get(i))) {
                      broadcastIds.add(idsByIntimacy.get(i));
                  }
                }

                return  broadcastIds;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

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

            HBaseConstant.saveSumIntimacy(rowKey, Convert.toInt(sumIntimacy), 5);

        } else {

            HBaseConstant.updateSumIntimacy(rowKey, intimacy, sumIntimacy, 5);
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

            HBaseConstant.saveSumIntimacy(rowKey, Convert.toInt(sumIntimacy), 8);

        }else {
            HBaseConstant.updateSumIntimacy(rowKey, intimacy, sumIntimacy, 8);
        }
        return true;
    }

    @Override
    public Boolean saveGiftRewardHBaseIntimacy(RequestGiftReward requestGiftReward) {
        Long broadcastId = requestGiftReward.getBroadcastId();
        Integer broadcastUserId = requestGiftReward.getBroadcastUserId();
        Integer giftRewardUserId = requestGiftReward.getGiftRewardUserId();

        String rowKey = broadcastUserId + "_"  +giftRewardUserId + ":" +  broadcastId;

        log.info("rowKey : {}", rowKey);

//        根据 rowKey 查询亲密度
        Result hBaseIntimacy = HBaseUtil.getRow(HBaseConstant.HBASE_BRO_TABLE, rowKey);

//        获取亲密度
        String intimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("intimacy"))
        );

//        获取总亲密度
        String sumIntimacy = Bytes.toString(
                hBaseIntimacy.getValue(Bytes.toBytes(HBaseConstant.HBASE_BRO_CF_DD), Bytes.toBytes("sumIntimacy")));

        if (intimacy == null) {

            HBaseConstant.saveSumIntimacy(rowKey, Convert.toInt(sumIntimacy), 10);

        } else {
            HBaseConstant.updateSumIntimacy(rowKey, intimacy, sumIntimacy, 10);
        }

        return true;
    }
}
