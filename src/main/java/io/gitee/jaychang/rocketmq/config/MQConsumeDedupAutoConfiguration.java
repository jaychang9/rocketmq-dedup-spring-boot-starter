package io.gitee.jaychang.rocketmq.config;

import io.gitee.jaychang.rocketmq.annotation.MQConsumer;
import io.gitee.jaychang.rocketmq.core.AbstractDedupMQConsumer;
import io.gitee.jaychang.rocketmq.core.DedupConfig;
import io.gitee.jaychang.rocketmq.core.PersistTypeEnum;
import io.gitee.jaychang.rocketmq.persist.JDBCPersist;
import io.gitee.jaychang.rocketmq.persist.RedisPersist;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jaychang
 * @description 消息消费防重自动配置
 * @date 2023/12/4
 **/
@Slf4j
@Configuration
@EnableConfigurationProperties({DedupProperties.class})
@AutoConfigureAfter({MQConsumerAutoConfiguration.class})
@ConditionalOnClass({DedupConfig.class})
public class MQConsumeDedupAutoConfiguration implements ApplicationContextAware {

    @SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
    @Autowired
    private DedupProperties dedupProperties;

    private ApplicationContext applicationContext;

    private ScheduledExecutorService scheduledExecutorService;

    public MQConsumeDedupAutoConfiguration() {
    }

    @Bean
    @ConditionalOnMissingBean
    public DedupConfig dedupConfig() {
        log.debug("消息消费防重配置");
        DedupConfig dedupConfig = new DedupConfig();
        dedupConfig.setApplicationName(dedupProperties.getApplicationName());
        dedupConfig.setDedupProcessingExpireMilliSeconds(dedupProperties.getDedupProcessingExpireMilliSeconds());
        dedupConfig.setDedupStrategy(DedupConfig.DEDUP_STRATEGY_CONSUME_LATER);
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
            // 不去重
            dedupConfig.setDedupStrategy(DedupConfig.DEDUP_STRATEGY_DISABLE);
        }

        return dedupConfig;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() throws Exception {
        final Map<String, Object> beans = applicationContext.getBeansWithAnnotation(MQConsumer.class);
        if (CollectionUtils.isEmpty(beans)) {
            return;
        }
        final DedupConfig dedupConfig = dedupConfig();
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object bean = entry.getValue();
            if (AbstractDedupMQConsumer.class.isAssignableFrom(bean.getClass())) {
                AbstractDedupMQConsumer dedupMQConsumer = (AbstractDedupMQConsumer) bean;
                dedupMQConsumer.setDedupConfig(dedupConfig);
            }
        }
        // 定时清理过期记录 (每24小时执行一次)
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().daemon(true).namingPattern("ClearExpiredRocketmqDedupThread-%d").build());
        scheduledExecutorService.scheduleWithFixedDelay(() -> dedupConfig.getPersist().clearExpiredRecord(), 24, 24, TimeUnit.HOURS);
    }
}
