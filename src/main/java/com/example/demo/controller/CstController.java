package com.example.demo.controller;

import cn.hutool.core.util.IdUtil;
import com.example.demo.entity.Item;
import com.example.demo.repo.ItemRepo;
import com.example.demo.service.CstService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@RestController
@Slf4j
public class CstController {

    @Resource
    private ItemRepo itemRepo;

    @Resource
    private CstService cstService;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private RedisTemplate<String,String> redisTemplate;
    @GetMapping("/")
    public String hello() {
        return "hello world";
    }

    @GetMapping("/copy")
    public String copy() throws InterruptedException {
        cstService.copy();
        return "hello world";
    }

    @GetMapping("/insert")
    public String insert() {
        jdbcTemplate.batchUpdate("delete from item");
        List<Item> items = new ArrayList<>();
        String sql = """
                        insert into item values(?,?,?,?);
                        """;
        for (int i = 0; i < 1000000; i++) {
            var item = new Item();
            item.setId(IdUtil.getSnowflake().nextId());
            item.setPacId(2L);
            item.setDataVersion(1L);
            item.setOrgCode("wdq");
            items.add(item);
            if(i%5000 == 0){
                log.info("times : {}" ,i);
                var tmpList = items;
                jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ps.setLong(1, tmpList.get(i).getId());
                        ps.setLong(2, tmpList.get(i).getDataVersion());
                        ps.setLong(3, tmpList.get(i).getPacId());
                        ps.setString(4, tmpList.get(i).getOrgCode());
                    }

                    @Override
                    public int getBatchSize() {
                        return tmpList.size();
                    }
                });
                items.clear();
            }
        }
        return "hello world";
    }

    @GetMapping("/test")
    public String test() throws InterruptedException {
        cstService.prepareCopy();
        cstService.prepareTest();
        return "hello world";
    }

    @GetMapping("/redis")
    public String redis(){
        Set<String> difference = redisTemplate.opsForSet().difference("set2", "set1");
        difference.forEach(System.out::println);
        return difference.toString();

    }

}
