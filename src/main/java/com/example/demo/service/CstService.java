package com.example.demo.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.thread.ExecutorBuilder;
import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONUtil;
import com.example.demo.entity.Item;
import com.example.demo.entity.RangeId;
import com.example.demo.entity.RangeList;
import com.example.demo.repo.ItemRepo;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

@Service
@Slf4j
public class CstService {
    private static final Executor executor = ExecutorBuilder.create().setCorePoolSize(10).setMaxPoolSize(15).build();
    @Resource
    private ItemRepo itemRepo;

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Resource
    private JdbcClient jdbcClient;

    @Resource
    private JdbcTemplate jdbcTemplate;

    public void saveAll(List<Item> items) {
        itemRepo.saveAll(items);
    }

    public void prepareCopy() {
        List<String> a = new ArrayList<>();
        redisTemplate.delete("copyList");
        long count = jdbcClient.sql("select count(*) from item").query(Long.class).single();
        Long minId = jdbcClient.sql("select id from item order by id limit 1").query(Long.class).single() - 1;
        long maxId = jdbcClient.sql("select id from item order by id desc limit 1").query(Long.class).single() + 1;
        for (int i = 0; i < count / 5000 + 1; i++) {
            var d = jdbcClient.sql("select id from item where id>? order by id  limit 5000").param(minId).query(Long.class).list();
            log.info(String.valueOf(d.size()));
            if (d.isEmpty()) {
                break;
            }
            long tmp = jdbcClient.sql("select id from item where id>? order by id limit 5000").param(minId).query(Long.class).list().getLast();
            var rangeId = new RangeId(minId, Math.min(maxId, tmp + 1));
            a.add(JSONUtil.toJsonStr(rangeId));
            minId = tmp + 1;
        }
        redisTemplate.opsForList().rightPushAll("copyList", a);
//        Lists.partition()
    }

    public void copy() throws InterruptedException {
        var startTime = System.currentTimeMillis();
        prepareCopy();
        log.info("prepare : {}s",(System.currentTimeMillis() - startTime) / 1000);
        var tmp = redisTemplate.opsForList().range("copyList", 0, -1);
        assert tmp != null;
        var rangeList = tmp.stream().map(it -> JSONUtil.toBean(it, RangeId.class)).toList();
        CountDownLatch countDownLatch = new CountDownLatch(rangeList.size());
        for (int i = 0; i < rangeList.size(); i++) {
            int finalI = i;
            CompletableFuture.runAsync(() -> {
                String sql = """
                        insert into item values(?,?,?,?);
                        """;
                List<Item> itemList = jdbcClient.sql("select * from item where id>? and id<?").param(rangeList.get(finalI).getMinId()).param(rangeList.get(finalI).getMaxId()).query(Item.class).list();
                if (CollectionUtil.isEmpty(itemList)) {
                    countDownLatch.countDown();
                    return;
                }
                itemList.forEach(item -> item.setId(IdUtil.getSnowflake().nextId()));
                jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ps.setLong(1, itemList.get(i).getId());
                        ps.setLong(2, itemList.get(i).getDataVersion());
                        ps.setLong(3, itemList.get(i).getPacId());
                        ps.setString(4, itemList.get(i).getOrgCode());
                    }

                    @Override
                    public int getBatchSize() {
                        return itemList.size();
                    }
                });
                countDownLatch.countDown();
            }, executor);

        }
        countDownLatch.await();
        log.info("cost : {}s", (System.currentTimeMillis() - startTime) / 1000);


    }


}
