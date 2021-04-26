/*
 * 实现从HDFS读取文件，并将文件内容写入HBase的指定表格中
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

	// 定义一个configuration
	private Configuration configuration = null;

	public Configuration getConf() {
		return configuration;
	}

	public void setConf(Configuration conf) {
		configuration = conf;
	}

	public int run(String[] args) throws Exception {

		// 获取job对象
		Job job = Job.getInstance(configuration);

		// 获取jar包、mapper、reducer类 -- 3
		job.setJarByClass(FruitDriver.class);
		job.setMapperClass(FruitMapper.class);
		// Reducer类为TableReducer类，所以不能按照老办法设置job.setReducerClass(FruitReducer.class);
		TableMapReduceUtil.initTableReducerJob("fruit3", FruitReducer.class, job);

		// mapper的输出格式、最终的输出格式(已定)-- 2
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// 输入输出路径-- 1（输出路径已定）
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/input_fruit/fruit.tsv"));

		// 提交job
		boolean result = job.waitForCompletion(true);

		return result ? 0 : 1;
	}

}
