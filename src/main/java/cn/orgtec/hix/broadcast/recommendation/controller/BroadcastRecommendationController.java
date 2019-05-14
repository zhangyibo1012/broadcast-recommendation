package cn.orgtec.hix.broadcast.recommendation.controller;

import cn.orgtec.hix.broadcast.recommendation.dto.HBaseBroadcast;
import cn.orgtec.hix.broadcast.recommendation.service.BroadcastRecommendation;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 推荐服务控制器
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@RestController
@RequestMapping(value = "/recommendation")
@AllArgsConstructor
public class BroadcastRecommendationController {

    private final BroadcastRecommendation broadcastRecommendation;

    /**
     * 保存广播属性到 HBase
     *
     * @param hBaseBroadcast     HBaseBroadcast
     * @return                   true  false
     */
    @PostMapping(value = "/saveHBaseBroadcast")
    public Boolean  saveHBaseBroadcast(@RequestBody HBaseBroadcast baseBroadcast){
        return broadcastRecommendation.saveHBaseBroadcast(baseBroadcast);
    }

}
