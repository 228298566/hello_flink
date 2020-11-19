package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 15:20
 **/
// 第三方支付事件，例如微信，支付宝
case class PayEvent(orderId: String,
                    eventType: String,
                    eventTime: Long)