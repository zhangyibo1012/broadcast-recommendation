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
public class RequestFavor extends HBaseBroadcast {

    /**
     *  点赞者的用户 id
     */
    private Integer favorUserId;
}
