package cn.orgtec.hix.broadcast.recommendation.config;

import cn.orgtec.hix.broadcast.recommendation.util.HBaseConstant;
import cn.orgtec.hix.broadcast.recommendation.util.HBaseUtil;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

/**
 * 初始化 HBase 数据表
 *
 * @author Yibo Zhang
 * @date 2019/05/14
 */
@Component
public class InitHBaseTable implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
//        HBaseUtil.createTable(HBaseConstant.HBASE_BRO_TABLE, new String[]{HBaseConstant.HBASE_BRO_CF_SD , HBaseConstant.HBASE_BRO_CF_DD});
    }
}
