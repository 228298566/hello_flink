package model;

public class WordCount {

    public String word;
    public Long count;

    public static WordCount of(String word, Long count) {
        WordCount wordCount = new WordCount();
        wordCount.word = word;
        wordCount.count = count;
        return wordCount;
    }

    @Override
    public String toString() {
        return "word:" + word + " count:" + count;
    }
}