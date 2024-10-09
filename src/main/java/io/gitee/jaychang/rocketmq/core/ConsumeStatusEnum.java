package io.gitee.jaychang.rocketmq.core;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

/**
 * 消费状态枚举
 */
@Getter
@AllArgsConstructor
public enum ConsumeStatusEnum {

    CONSUMING(0, "CONSUMING"),
    CONSUMED(1, "CONSUMED");

    private final Integer code;
    private final String label;

    public static ConsumeStatusEnum codeOf(Integer code) {
        return Arrays.stream(ConsumeStatusEnum.values()).filter(e -> e.getCode().equals(code)).findFirst().orElse(null);
    }
}
