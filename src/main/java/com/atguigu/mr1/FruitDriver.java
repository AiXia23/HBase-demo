/*
 * ʵ�ִ�HDFS��ȡ�ļ��������ļ�����д��HBase��ָ�������
 */

package com.atguigu.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			int run = ToolRunner.run(conf, new FruitDriver(), args);
			System.exit(run);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// ����һ��configuration
	private Configuration configuration = null;

	public Configuration getConf() {
		return configuration;
	}

	public void setConf(Configuration conf) {
		configuration = conf;
	}

	public int run(String[] args) throws Exception {

		// ��ȡjob����
		Job job = Job.getInstance(configuration);

		// ��ȡjar����mapper��reducer�� -- 3
		job.setJarByClass(FruitDriver.class);
		job.setMapperClass(FruitMapper.class);
		// Reducer��ΪTableReducer�࣬���Բ��ܰ����ϰ취����job.setReducerClass(FruitReducer.class);
		TableMapReduceUtil.initTableReducerJob("fruit3", FruitReducer.class, job);

		// mapper�������ʽ�����յ������ʽ(�Ѷ�)-- 2
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// �������·��-- 1�����·���Ѷ���
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/input_fruit/fruit.tsv"));

		// �ύjob
		boolean result = job.waitForCompletion(true);

		return result ? 0 : 1;
	}

}
