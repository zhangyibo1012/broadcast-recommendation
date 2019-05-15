package cn.orgtec.hix.broadcast.recommendation.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString
public class RequestGiftReward extends HBaseBroadcast {

    /**
     *  打赏者的用户 id
     */
    private Integer giftRewardUserId;
}
