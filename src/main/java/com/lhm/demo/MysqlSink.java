package com.lhm.demo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

public class MysqlSink extends RichSinkFunction<List<person>> {

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取数据库连接，准备写入数据库
        connection = DBUtils.getConnection();
        String sql = "insert into person(id, name, age, createDate) values (?, ?, ?, ?); ";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<person> persons, Context context) throws Exception {
        for(person person : persons) {
            ps.setInt(1, person.getId());
            ps.setString(2, person.getName());
            ps.setInt(3, person.getAge());
            ps.setTimestamp(4, new Timestamp(person.getCreateDate().getTime()));
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("成功写入Mysql数量：" + count.length);

    }

}
