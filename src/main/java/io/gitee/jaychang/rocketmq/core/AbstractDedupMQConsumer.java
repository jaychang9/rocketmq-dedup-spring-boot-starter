package io.gitee.jaychang.rocketmq.core;


import io.gitee.jaychang.rocketmq.strategy.ConsumeStrategy;
import io.gitee.jaychang.rocketmq.strategy.DedupConsumeStrategy;
import io.gitee.jaychang.rocketmq.strategy.NormalConsumeStrategy;
import com.maihaoche.starter.mq.base.AbstractMQPushConsumer;
import com.maihaoche.starter.mq.base.MessageExtConst;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.BiFunction;


/**
 * 带去重逻辑的通用消费者，实现者需要实现doHandleMsg
 * 支持消息幂等的策略
 */
@Slf4j
public abstract class AbstractDedupMQConsumer<T> extends AbstractMQPushConsumer<T> {

    // 默认不去重
    protected DedupConfig dedupConfig = DedupConfig.disableDupConsumeConfig("NOT-SET-CONSUMER-GROUP");


    /**
     * 默认不去重
     */
    public AbstractDedupMQConsumer() {
        log.info("Construct AbstractDedupMQConsumer with default {}", dedupConfig);
    }

    /**
     * 设置去重策略
     *
     * @param dedupConfig
     */
    public AbstractDedupMQConsumer(DedupConfig dedupConfig) {
        this.dedupConfig = dedupConfig;
        log.info("Construct AbstractDedupMQConsumer with dedupConfig {}", dedupConfig);
    }


    /**
     * 消息消费，带去重逻辑
     *
     * @param message 消息范型
     * @param extMap  存放消息附加属性的map, map中的key存放在 @link MessageExtConst 中
     * @return
     */
    @Override
    public boolean process(T message, Map<String, Object> extMap) {
        ConsumeStrategy strategy = new NormalConsumeStrategy();

        BiFunction<T, Map<String, Object>, String> dedupKeyFunction = this::dedupMessageKey;

        if (dedupConfig.getDedupStrategy() == DedupConfig.DEDUP_STRATEGY_CONSUME_LATER) {
            strategy = new DedupConsumeStrategy(dedupConfig, (BiFunction<Object, Map<String, Object>, String>) dedupKeyFunction);
        }
        BiFunction<T, Map<String, Object>, Boolean> doProcessFun = AbstractDedupMQConsumer.this::doProcess;
        //调用对应的策略
        return strategy.invoke(doProcessFun, message, extMap);
    }

    /**
     * 子类实现此方法。真正处理消息
     *
     * @param extMap
     * @return true表示消费成功，false表示消费失败
     */
    protected abstract boolean doProcess(T message, Map<String, Object> extMap);


    /**
     * 默认拿uniqkey 作为去重的标识
     */
    protected String dedupMessageKey(final T message, final Map<String, Object> extMap) {
        String uniqID = (String) extMap.get(MessageExtConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqID == null) {
            return (String) extMap.get(MessageExtConst.PROPERTY_EXT_MSG_ID);
        } else {
            return uniqID;
        }
    }

    public void setDedupConfig(DedupConfig dedupConfig) {
        this.dedupConfig = dedupConfig;
    }
}



