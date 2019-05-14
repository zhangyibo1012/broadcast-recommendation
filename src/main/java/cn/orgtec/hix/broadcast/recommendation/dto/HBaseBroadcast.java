package cn.orgtec.hix.broadcast.recommendation.dto;

import lombok.Data;
import lombok.ToString;

/**
 * HBaseBroadcast
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Data
@ToString
public class HBaseBroadcast {

    /**
     *  广播 id
     */
    private Long broadcastId;

    /**
     *  该广播的用户 id
     */
    private Integer broadcastUserId;
}
