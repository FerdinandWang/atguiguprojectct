package com.atguigu.ct.consumer.coprocessor;

import com.atguigu.ct.common.bean.BaseDao;
import com.atguigu.ct.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * 使用协处理器保存被叫用户的数据
 * --协处理器的使用
 * 1.创建类
 * 2.让表知道协处理类(和表有关联),在表的描述器中添加该类
 * 3.将项目打成jar包发布到hbase/lib中(关联的jar包也需要发布),并且需要分发到每个节点
 *
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {
    /**
     * 保存主叫用户数据之后,有Hbase自动保存被叫用户数据
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());

        System.out.println("看这里-----------------------------------看这里"+rowkey);

        //1_13312341234_20190101_13212341234_0222_1
        String values[] = rowkey.split("_");

//        InsertCalleeCoprocessor.CoprocessorDao coprocessorDao = new InsertCalleeCoprocessor(). new CoprocessorDao();
        CoprocessorDao coprocessorDao = new CoprocessorDao();

        String call1 = values[1];
        String call2 = values[3];
        String calltime = values[2];
        String duration = values[4];
        String flag = values[5];

        //只有主叫用户保存后才需要出发被叫用户的保存
        if("1".equals(flag)){
            String calleeRowkey = coprocessorDao.getRegionNum(call2, calltime) + "_" + call2 + "_" + calltime + "_" +call1 + "_" + duration + "_" + "0";

            //被叫用户
            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLER.getValue());

            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));

            //保存数据
            table.put(calleePut);

            //关闭表
            table.close();
        }

    }


    private class CoprocessorDao extends BaseDao{
        public int getRegionNum(String tel, String time){
            return genregionNum(tel, time);
        }
    }


}
