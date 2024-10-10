package cn.jaychang.rocketmq.strategy;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
@AllArgsConstructor
public class NormalConsumeStrategy implements ConsumeStrategy {

    @Override
    public <T> boolean invoke(BiFunction<T, Map<String, Object>, Boolean> consumeCallback, T message, Map<String, Object> extMap) {
        return consumeCallback.apply(message, extMap);
    }
}