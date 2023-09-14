package io.datadynamics.wordlen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordLengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable wordLength = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\\s+");

        for (String w : words) {
            // 단어의 길이를 계산하여 IntWritable에 설정
            wordLength.set(w.length());
            // 컨텍스트에 (단어 길이, 1) 쌍을 출력
            context.write(wordLength, one);
        }
    }
}
