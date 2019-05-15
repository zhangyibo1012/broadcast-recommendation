package cn.orgtec.hix.broadcast.recommendation.service;

import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestComment;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestFavor;
import cn.orgtec.hix.broadcast.recommendation.dto.RequestGiftReward;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Service
public interface BroadcastRecommendation {

    /**
     * 根据亲密度获取广播 id
     *
     * @param userId  用户 id
     * @return        List<Long>
     */
    List<Long> getRecommendedBroadcastIds(Integer userId);

    /**
     * 保存广播属性到 HBase
     *
     * @param hBaseBroadcast     HBaseBroadcast
     * @return                   true  false
     */
    Boolean saveHBaseBroadcast(HBaseBroadcast hBaseBroadcast );

    /**
     * 保存或更新点赞的亲密度
     *
     * @param requestFavor    RequestFavor
     * @return                  true  false
     */
    Boolean saveFavorHBaseIntimacy(RequestFavor requestFavor);

    /**
     * 保存或更新评论的亲密度
     *
     * @param requestComment    RequestComment
     * @return                  true  false
     */
    Boolean saveCommentHBaseIntimacy(RequestComment requestComment);

    /**
     * 保存或更新评论的亲密度
     *
     * @param requestComment    RequestComment
     * @return                  true  false
     */
    Boolean saveGiftRewardHBaseIntimacy(RequestGiftReward requestGiftReward);

}
