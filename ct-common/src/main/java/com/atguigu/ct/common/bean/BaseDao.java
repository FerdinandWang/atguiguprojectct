package com.atguigu.ct.common.bean;

import com.atguigu.ct.common.api.Column;
import com.atguigu.ct.common.api.Rowkey;
import com.atguigu.ct.common.api.TableRef;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;


/**
 * 基础的数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws Exception{
        getConnection();
        getAdmin();
    }

    /**
     * 结束连接
     * @throws Exception
     */
    protected void end() throws Exception{
        Admin admin = getAdmin();
        if (admin != null){
            admin.close();
            adminHolder.remove();
        }

        Connection connection = getConnection();
        if (connection != null){
            connection.close();
            connHolder.remove();
        }

    }


    /**
     * 创建表
     * 如果表已经存在,那么删除
     * @param name
     * @param families
     */
    protected void createTableXX(String name, String... families) throws Exception {
        createTableXX(name, null, families);
    }

    protected void createTableXX(String name, Integer regionCount, String... families) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        if(admin.tableExists(tableName)){
            //表存在,删除
            deleteTable(name);
        }
        //创建表
        createTable(name, regionCount, families);
    }

    private void createTable(String name, Integer regionCount,String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if(families == null || families.length == 0){
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        //增加预分区
        //分区键
        if(regionCount == null || regionCount <= 1){
            admin.createTable(tableDescriptor);
        }else {
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }

    }

    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(int regionCount){
        int splitkeyCount = regionCount - 1;
        byte[][] bs = new byte[splitkeyCount][];
        //0,1,2,3,4
        List<byte[]> bsList = new ArrayList<byte[]>();
        for (int i = 0; i < splitkeyCount ; i++) {
            String splitkey = i + "|";
            bsList.add(Bytes.toBytes(splitkey));
        }

        bsList.toArray(bs);

        return bs;
    }

    /**
     * 计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genregionNum(String tel, String date){
        //13712345678
        String usercode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);

        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //crc校验采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        //取模
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    /**
     * 增加对象:自动封装数据,将对象数据直接保存到hbase中去
     * @param obj
     * @throws Exception
     */
    protected void putData(Object obj) throws Exception {

        //反射
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String rowkeyStr = "";
        for (Field f : fs) {
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if(rowkey != null){
                f.setAccessible(true);
                rowkeyStr = (String) f.get(obj);
                break;
            }
        }

        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkeyStr));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if(column != null){
                String family = column.family();
                String colName = column.column();
                if(colName == null || "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String) f.get(obj);

                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    /**
     * 增加数据
     * @param name
     * @param put
     */
    protected void putData(String name, Put put) throws Exception {


        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    /**
     * 删除表
     * @param name
     * @throws Exception
     */
    protected void deleteTable(String name) throws Exception{
        TableName tableName = TableName.valueOf(name);
        Admin admin = getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建命名空间,如果命名空间已经存在,则不需要创建,否则,创建新的
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws Exception {
        Admin admin = getAdmin();
        try{
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
//            e.printStackTrace();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }

    }

    /**
     * 获取管理对象
     * 访问权限:只能子类访问
     */
    protected synchronized Admin getAdmin() throws Exception {
        Admin admin = adminHolder.get();
        if(admin == null){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接
     * 访问权限:只能子类访问
     */
    protected synchronized Connection getConnection() throws Exception {
        Connection conn = connHolder.get();
        if(conn == null){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection();
            connHolder.set(conn);
        }

        return conn;
    }

}
