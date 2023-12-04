package com.zcckj.plugin.rocketmq.config;

import com.zcckj.plugin.rocketmq.core.DedupConfig;
import com.zcckj.plugin.rocketmq.core.PersistTypeEnum;
import com.zcckj.plugin.rocketmq.persist.JDBCPersist;
import com.zcckj.plugin.rocketmq.persist.RedisPersist;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Objects;

/**
 * @author jaychang
 * @description 消息消费防重自动配置
 * @date 2023/12/4
 **/
@Configuration
@EnableConfigurationProperties({DedupProperties.class})
@ConditionalOnClass({DedupConfig.class})
public class MQConsumeDedupAutoConfiguration implements ApplicationContextAware {

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private DedupProperties dedupProperties;

    private ApplicationContext applicationContext;

    public MQConsumeDedupAutoConfiguration() {
    }

    @Bean
    @ConditionalOnMissingBean
    public DedupConfig dedupConfig() {
        DedupConfig dedupConfig = new DedupConfig();
        dedupConfig.setApplicationName(dedupProperties.getApplicationName());
        dedupConfig.setDedupProcessingExpireMilliSeconds(dedupProperties.getDedupProcessingExpireMilliSeconds());
        dedupConfig.setDedupRecordReserveMinutes(dedupProperties.getDedupRecordReserveMinutes());

        PersistTypeEnum persistType = dedupProperties.getPersistType();
        if (PersistTypeEnum.DB.equals(persistType)) {
            JdbcTemplate jdbcTemplate = applicationContext.getBean(JdbcTemplate.class);
            if (Objects.isNull(jdbcTemplate)) {
                throw new RuntimeException("Can not found JdbcTemplate bean in spring context");
            }
            JDBCPersist jdbcPersist = new JDBCPersist(jdbcTemplate);
            dedupConfig.setPersist(jdbcPersist);
        } else if (PersistTypeEnum.REDIS.equals(persistType)) {
            StringRedisTemplate stringRedisTemplate = applicationContext.getBean(StringRedisTemplate.class);
            if (Objects.isNull(stringRedisTemplate)) {
                throw new RuntimeException("Can not found StringRedisTemplate bean in spring context");
            }
            RedisPersist redisPersist = new RedisPersist(stringRedisTemplate);
            dedupConfig.setPersist(redisPersist);
        } else {
            throw new UnsupportedOperationException("Unknown persist type");
        }

        return dedupConfig;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
