package io.datadynamics.wordlen;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordLengthPartitioner extends Partitioner<IntWritable, IntWritable> {

    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        // 워드 길이를 기준으로 파티션을 결정
        int wordLength = key.get();

        // 파티션 개수가 3개이므로 3으로 나눈 나머지로 파티션 결정
        return wordLength % numPartitions;
    }
}

