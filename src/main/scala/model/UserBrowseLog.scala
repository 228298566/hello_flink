package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 13:51
 **/
// 用户浏览日志
case class UserBrowseLog(userID: String,
                         eventTime: String,
                         eventType: String,
                         productID: String,
                         productPrice: String)
