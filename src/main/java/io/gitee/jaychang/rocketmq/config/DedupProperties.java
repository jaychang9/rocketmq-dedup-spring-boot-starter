package io.gitee.jaychang.rocketmq.config;

import io.gitee.jaychang.rocketmq.core.PersistTypeEnum;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;

/**
 * @author jaychang
 * @description 消息消费防重配置类
 **/
@Data
@Configuration
@ConfigurationProperties(prefix = "rocketmq.consume.dedup")
public class DedupProperties implements Serializable {
    private static final long serialVersionUID = -5942739731628880991L;

    private PersistTypeEnum persistType = PersistTypeEnum.DB;

    private String applicationName;

    /**
     * 对于消费中的消息，多少毫秒内认为重复，默认30分钟，即30分钟内的重复消息都会串行处理（等待前一个消息消费成功/失败），超过这个时间如果消息还在消费就不认为重复了（为了防止消息丢失）
     */
    private long dedupProcessingExpireMilliSeconds = 1000 * 60 * 30;

    /**
     * 消息消费成功后，记录保留多少分钟，默认180天，即180天内的消息不会重复
     */
    private long dedupRecordReserveMinutes = 60 * 24 * 180;
}
