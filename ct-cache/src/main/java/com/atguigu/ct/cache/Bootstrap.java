package com.atguigu.ct.cache;

import com.atguigu.ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 启动客户端,向redis中增加缓存数据
 */
public class Bootstrap {

    public static void main(String[] args) {

        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dateMap = new HashMap<String, Integer>();
        //读取Mysql中的数据
        String queryUserSql = "select id, tel from ct_user";
        Connection connection = null;
        PreparedStatement pstat = null;
        ResultSet rs = null;
        try {
            connection = JDBCUtil.getConnection();
            pstat = connection.prepareStatement(queryUserSql);
            rs = pstat.executeQuery();
            while (rs.next()){
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel, id);
            }
            rs.close();

            String queryDateSql = "select id, year, month, day from ct_date";
            pstat = connection.prepareStatement(queryDateSql);
            rs = pstat.executeQuery(queryDateSql);

            while(rs.next()){
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if (month.length() == 1){
                    month = "0" + month;
                }
                String day = rs.getString(4);
                if(day.length() == 1){
                    day = "0" + day;
                }
                dateMap.put(year+month+day, id);
            }


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }  finally {
            if (rs != null){
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (pstat != null){
                try {
                    pstat.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

//        System.out.println(userMap.size());
//        System.out.println(dateMap.size());

        //向redis中存储数据
        Jedis jedis = new Jedis("hadoop01", 6379);
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, "" + value);
        }

        keyIterator = dateMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key = keyIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date", key, "" + value);
        }

    }

}
