package cn.orgtec.hix.broadcast.recommendation.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * RequestComment
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString
public class RequestComment extends HBaseBroadcast {

    /**
     * 评论者的用户 id
     */
    private Integer commentUserId;
}
