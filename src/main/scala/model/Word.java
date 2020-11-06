package model;


/**
 * Desc:
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

public class Word {
    public String word;
    public Long timestamp;

    public Word() {
    }

    public Word(String word,Long timestamp) {
        this.word = word;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
