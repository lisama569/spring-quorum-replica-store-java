package com.github.lisama569.springquorumreplicastorejava.rpc;

/**
 * 投递中的消息信封。
 *
 * @param from    发送方
 * @param to      接收方
 * @param message 消息体
 * @param seq     总线分配的单调序号（用于乱序/重复注入的可解释性）
 */
public record Envelope(String from, String to, Message message, long seq) {
}
