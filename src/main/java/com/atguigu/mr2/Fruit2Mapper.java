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
		
		//����put����
		Put put = new Put(key.get());
		
		//��ȡÿһcell������
		for (Cell cell : value.rawCells()) {
			//ɸѡ����ǰ��rowkey�еĵġ�name����
			if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
				//��put������Ӳ���
				put.add(cell);	
			}
		}
		
		//д������
		context.write(key, put);
	}
}
