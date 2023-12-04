package com.zcckj.plugin.rocketmq.strategy;


import com.maihaoche.starter.mq.base.MessageExtConst;
import com.zcckj.plugin.rocketmq.core.ConsumeStatusEnum;
import com.zcckj.plugin.rocketmq.core.DedupConfig;
import com.zcckj.plugin.rocketmq.persist.DedupElement;
import com.zcckj.plugin.rocketmq.persist.IPersist;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 去重策略的消费策略，去重数据存储目前支持MySQL（JDBC）和Redis，详见 persist包下的实现类
 * 1.如果已经消费过，则直接消费幂等掉
 * 2.如果正在消费中，则消费会延迟消费（consume later）注：如果一直消费中，由于需要避免消息丢失，即使前一个消息没消费结束依然会消费
 */
@Slf4j
@AllArgsConstructor
public class DedupConsumeStrategy implements ConsumeStrategy {


    private final DedupConfig dedupConfig;

    //获取去重键的函数
    private final Function<Map<String, Object>, String> dedupMessageKeyFunction;


    @Override
    public <T> boolean invoke(BiFunction<T, Map<String, Object>, Boolean> consumeCallback, T message, Map<String, Object> extMap) {
        IPersist persist = dedupConfig.getPersist();
        final String topic = (String) extMap.get(MessageExtConst.PROPERTY_TOPIC);
        final String tags = (String) extMap.getOrDefault(MessageExtConst.PROPERTY_TAGS, "");
        DedupElement dedupElement = new DedupElement(dedupConfig.getApplicationName(), topic, tags, dedupMessageKeyFunction.apply(extMap));
        Boolean shouldConsume = true;

        if (dedupElement.getMsgUniqKey() != null) {
            shouldConsume = persist.setConsumingIfNX(dedupElement, dedupConfig.getDedupProcessingExpireMilliSeconds());
        }

        //设置成功，证明应该要消费
        if (shouldConsume != null && shouldConsume) {
            //开始消费
            return doHandleMsgAndUpdateStatus(consumeCallback, message, extMap, dedupElement);
        } else {//有消费过/中的，做对应策略处理
            Integer val = persist.getConsumeStatus(dedupElement);
            final ConsumeStatusEnum consumeStatusEnum = ConsumeStatusEnum.codeOf(val);
            final String msgId = (String) extMap.get(MessageExtConst.PROPERTY_EXT_MSG_ID);

            if (ConsumeStatusEnum.CONSUMING.equals(consumeStatusEnum)) {//正在消费中，稍后重试
                log.warn("the same message is considered consuming, try consume later dedupKey : {}, {}, {}", persist.toPrintInfo(dedupElement), msgId, persist.getClass().getSimpleName());
                return false;
            } else if (ConsumeStatusEnum.CONSUMED.equals(consumeStatusEnum)) {//证明消费过了，直接消费认为成功
                log.warn("message has been consumed before! dedupKey : {}, msgId : {} , so just ack. {}", persist.toPrintInfo(dedupElement), msgId, persist.getClass().getSimpleName());
                return true;
            } else {//非法结果，降级，直接消费
                log.warn("[NOTIFYME]unknown consume result {}, ignore dedup, continue consuming,  dedupKey : {}, {}, {} ", val, persist.toPrintInfo(dedupElement), msgId, persist.getClass().getSimpleName());
                return doHandleMsgAndUpdateStatus(consumeCallback, message, extMap, dedupElement);
            }
        }

    }

    /**
     * 消费消息，末尾消费失败会删除消费记录，消费成功则更新消费状态
     */
    private <T> boolean doHandleMsgAndUpdateStatus(final BiFunction<T, Map<String, Object>, Boolean> consumeCallback, final T message, final Map<String, Object> extMap, final DedupElement dedupElement) {

        final String msgId = (String) extMap.get(MessageExtConst.PROPERTY_EXT_MSG_ID);
        if (dedupElement.getMsgUniqKey() == null) {
            log.warn("dedup key is null , consume msg but not update status{}", msgId);
            return consumeCallback.apply(message, extMap);
        } else {
            IPersist persist = dedupConfig.getPersist();
            boolean consumeRes = false;
            try {
                consumeRes = consumeCallback.apply(message, extMap);
            } catch (Throwable e) {
                //消费失败了，删除这个key
                try {
                    persist.delete(dedupElement);
                } catch (Exception ex) {
                    log.error("error when delete dedup record {}", dedupElement, ex);
                }
                throw e;
            }


            //没有异常，正常返回的话，判断消费结果
            try {
                if (consumeRes) {//标记为这个消息消费过
                    log.debug("set consume res as CONSUME_STATUS_CONSUMED , {}", dedupElement);
                    persist.markConsumed(dedupElement, dedupConfig.getDedupRecordReserveMinutes());
                } else {
                    log.info("consume Res is false, try deleting dedup record {} , {}", dedupElement, persist);
                    persist.delete(dedupElement);//消费失败了，删除这个key
                }
            } catch (Exception e) {
                log.error("消费去重收尾工作异常 {}，忽略异常", msgId, e);
            }
            return consumeRes;
        }

    }


}