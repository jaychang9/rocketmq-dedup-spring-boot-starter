package com.zcckj.plugin.rocketmq.persist;


import com.zcckj.plugin.rocketmq.core.ConsumeStatusEnum;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;

import java.util.concurrent.TimeUnit;

/**
 * Created by linjunjie1103@gmail.com
 */

public class RedisPersist implements IPersist {
    private final StringRedisTemplate redisTemplate;
    private static final String COLON = ":";

    public RedisPersist(StringRedisTemplate redisTemplate) {
        if (redisTemplate == null) {
            throw new NullPointerException("redis template is null");
        }
        this.redisTemplate = redisTemplate;
    }


    @Override
    public boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement);
        //setnx, 成功就可以消费
        Boolean execute = redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> redisConnection.set(dedupKey.getBytes(), (String.valueOf(ConsumeStatusEnum.CONSUMED.getCode())).getBytes(), Expiration.milliseconds(dedupProcessingExpireMilliSeconds), RedisStringCommands.SetOption.SET_IF_ABSENT));

        if (execute == null) {
            return false;
        }

        return execute;
    }

    @Override
    public void delete(DedupElement dedupElement) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement);
        redisTemplate.delete(dedupKey);
    }

    @Override
    public void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement);
        redisTemplate.opsForValue().set(dedupKey, String.valueOf(ConsumeStatusEnum.CONSUMING.getCode()), dedupRecordReserveMinutes, TimeUnit.MINUTES);

    }

    @Override
    public Integer getConsumeStatus(DedupElement dedupElement) {
        String dedupKey = buildDedupMessageRedisKey(dedupElement);
        return Integer.valueOf(redisTemplate.opsForValue().get(dedupKey));
    }

    @Override
    public String toPrintInfo(DedupElement dedupElement) {
        return buildDedupMessageRedisKey(dedupElement);
    }

    private String buildDedupMessageRedisKey(DedupElement dedupElement) {
        if (StringUtils.isEmpty(dedupElement.getMsgUniqKey())) {
            return null;
        } else {
            //示例：MQ:CONSUME_DEDUP:APPNAME:TOPIC:TAG:APP_DEDUP_KEY
            String tag = StringUtils.isNotBlank(dedupElement.getTag()) ? COLON + dedupElement.getTag() : "";
            String prefix = "MQ:CONSUME_DEDUP:" + dedupElement.getApplication() + COLON + dedupElement.getTopic() + tag + COLON + dedupElement.getConsumerGroup();
            return prefix + COLON + dedupElement.getMsgUniqKey();
        }
    }


}
