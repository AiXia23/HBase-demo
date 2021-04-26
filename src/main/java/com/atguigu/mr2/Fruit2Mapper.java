package com.atguigu.mr2;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		//构建put对象
		Put put = new Put(key.get());
		
		//获取每一cell的数据
		for (Cell cell : value.rawCells()) {
			//筛选出当前的rowkey中的的“name”列
			if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
				//给put对象添加参数
				put.add(cell);	
			}
		}
		
		//写出数据
		context.write(key, put);
	}
}
