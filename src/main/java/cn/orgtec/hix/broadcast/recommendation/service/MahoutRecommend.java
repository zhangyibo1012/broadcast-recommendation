package cn.orgtec.hix.broadcast.recommendation.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.mahout.cf.taste.model.DataModel;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/06/05
 */
@Service
@Slf4j
@AllArgsConstructor
public class MahoutRecommend {

    public List<Long> getRecommendBroadcastId(){
        DataModel dataModel = null;
        return null;
    }
}
