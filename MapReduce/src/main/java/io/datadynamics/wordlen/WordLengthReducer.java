package io.datadynamics.wordlen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordLengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // 리듀서에서는 단어 길이별 빈도수를 출력
        context.write(key, new Text(String.valueOf(sum)));
    }
}
