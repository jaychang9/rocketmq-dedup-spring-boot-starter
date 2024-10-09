package io.gitee.jaychang.rocketmq.persist;


public interface IPersist {
    boolean setConsumingIfNX(DedupElement dedupElement, long dedupProcessingExpireMilliSeconds);

    void delete(DedupElement dedupElement);

    void markConsumed(DedupElement dedupElement, long dedupRecordReserveMinutes);

    Integer getConsumeStatus(DedupElement dedupElement);

    default String toPrintInfo(DedupElement dedupElement) {
        return dedupElement.toString();
    }

    default void clearExpiredRecord() {}
}
