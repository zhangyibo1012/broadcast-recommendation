package cn.orgtec.hix.broadcast.recommendation.service;

import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import org.springframework.stereotype.Service;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Service
public interface BroadcastRecommendation {

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
     * @param hBaseBroadcast    HBaseBroadcast
     * @return                  true  false
     */
    Boolean saveFavorHBaseIntimacy(HBaseBroadcast hBaseBroadcast );

}
