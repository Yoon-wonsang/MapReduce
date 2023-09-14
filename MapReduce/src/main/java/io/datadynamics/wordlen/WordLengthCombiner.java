package io.datadynamics.wordlen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordLengthCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // 컴바이너에서는 같은 길이의 단어 빈도수를 합쳐서 출력
        context.write(key, new IntWritable(sum));
    }
}