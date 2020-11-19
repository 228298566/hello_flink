package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 15:20
 **/
// 订单支付事件
case class OrderEvent(orderId: String,
                      eventType: String,
                      eventTime: Long)
