package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 13:51
 **/
// 用户点击日志
case class UserClickLog(userID: String,
                        eventTime: String,
                        eventType: String,
                        pageID: String)
