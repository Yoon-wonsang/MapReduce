package io.datadynamics.wordlen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthMain {

    public static void main(String[] args) throws Exception {
        // 하둡 설정을 가져옴
        Configuration conf = new Configuration();

        // 잡(Job) 객체 생성
        Job job = Job.getInstance(conf, "wordlengthcount");
        job.setJarByClass(WordLengthMain.class);

        // 매퍼, 리듀서, 컴바이너 설정
        job.setMapperClass(WordLengthMapper.class);
        job.setCombinerClass(WordLengthCombiner.class);
        job.setReducerClass(WordLengthReducer.class);

        // 맵 출력 타입 설정
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 리듀스 출력 타입 설정
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 파티셔너 설정
        job.setPartitionerClass(WordLengthPartitioner.class);

        // 입력과 출력 경로 설정
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 잡 실행 및 완료 대기
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
