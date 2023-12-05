package com.zcckj.plugin.rocketmq.persist;


import com.zcckj.plugin.rocketmq.core.ConsumeStatusEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;

/**
 * 需要创建如下表结构
 *<p>
     DROP TABLE IF EXISTS `t_rocketmq_dedup`;
     CREATE TABLE `t_rocketmq_dedup` (
     `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
     `create_time` datetime NOT NULL COMMENT '创建时间',
     `update_time` datetime NOT NULL COMMENT '更新时间',
     `application_name` varchar(32) NOT NULL COMMENT '应用名',
     `topic` varchar(64) NOT NULL COMMENT '消息Topic',
     `tag` varchar(32) NOT NULL COMMENT '消息Tag',
     `msg_uniq_key` varchar(64) NOT NULL COMMENT '消息Key',
     `consume_status` tinyint(1) NOT NULL COMMENT '消费状态：【0=消费中，1=已消费】',
     `expire_time` bigint(20) NOT NULL COMMENT '过期时间【时间戳，单位：毫秒，如果状态是消费中，但已过了过期时间，则可删除该记录】',
     PRIMARY KEY (`id`),
     UNIQUE KEY `uk_uniq_key` (`application_name`,`topic`,`tag`,`msg_uniq_key`),
     KEY `idx_expire_time` (`expire_time`)
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='消息防重消费表';
 *</p>
 *
 */
@Slf4j
public class JDBCPersist implements IPersist {
    private final JdbcTemplate jdbcTemplate;

    private final static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public JDBCPersist(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds) {
        long expireTime = System.currentTimeMillis() + dedupProcessingExpireMilliSeconds;
        try {
            String dateTimeStr = DateFormatUtils.format(new Date(), DATE_TIME_FORMAT);
            int i = jdbcTemplate.update("INSERT INTO t_rocketmq_dedup(create_time,update_time,application_name, topic, tag, msg_uniq_key, consume_status, expire_time) values (?, ?, ?, ?, ?, ?, ?, ?)", dateTimeStr, dateTimeStr, dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), ConsumeStatusEnum.CONSUMING.getCode(), expireTime);
        } catch (org.springframework.dao.DuplicateKeyException e) {
            log.warn("found consuming/consumed record, set setConsumingIfNX fail {}", dedupElement);

            /**
             * 由于mysql不支持消息过期，出现重复主键的情况下，有可能是过期的一些记录，这里动态的删除这些记录后重试
             */
            int i = delete(dedupElement, true);
            if (i > 0) {//如果删除了过期消息
                log.info("delete {} expire records, now retry setConsumingIfNX again", i);
                return setConsumingIfNX(dedupElement, dedupProcessingExpireMilliSeconds);
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error("unknown error when jdbc insert, will consider success", e);
            return true;
        }

        //插入成功则返回true
        return true;
    }


    private int delete(DedupElement dedupElement, boolean onlyExpire) {
        if (onlyExpire) {
            return jdbcTemplate.update("DELETE FROM t_rocketmq_dedup  WHERE application_name = ? AND topic =? AND tag = ? AND msg_uniq_key = ? AND expire_time < ?", dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), System.currentTimeMillis());
        } else {
            return jdbcTemplate.update("DELETE FROM t_rocketmq_dedup  WHERE application_name = ? AND topic =? AND tag = ? AND msg_uniq_key = ?", dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());
        }
    }

    @Override
    public void delete(DedupElement dedupElement) {
        delete(dedupElement, false);
    }


    @Override
    public void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes) {
        long expireTime = System.currentTimeMillis() + dedupRecordReserveMinutes * 60 * 1000;
        String dateTimeStr = DateFormatUtils.format(new Date(), DATE_TIME_FORMAT);
        int i = jdbcTemplate.update("UPDATE t_rocketmq_dedup SET update_time = ? ,consume_status = ? , expire_time  = ? WHERE application_name = ? AND topic = ? AND tag = ? AND msg_uniq_key = ? ",
                dateTimeStr, ConsumeStatusEnum.CONSUMED.getCode(), expireTime, dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey());
    }

    @Override
    public Integer getConsumeStatus(DedupElement dedupElement) {
        Integer consumeStatus = jdbcTemplate.queryForObject("SELECT consume_status FROM t_rocketmq_dedup where application_name = ? AND topic = ? AND tag = ? AND msg_uniq_key  = ? and expire_time > ?",
                new Object[]{dedupElement.getApplication(), dedupElement.getTopic(), dedupElement.getTag(), dedupElement.getMsgUniqKey(), System.currentTimeMillis()}, Integer.class);
        return consumeStatus;
    }

    @Override
    public void clearExpiredRecord() {
        int update = jdbcTemplate.update("DELETE FROM t_rocketmq_dedup WHERE expire_time < ? AND consume_status = ?", System.currentTimeMillis(), ConsumeStatusEnum.CONSUMED.getCode());
        log.debug("{} record has been removed.", update);
    }
}
