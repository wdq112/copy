package com.example.demo.service;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.thread.ExecutorBuilder;
import cn.hutool.core.util.IdUtil;
import com.example.demo.entity.CstVO;
import com.example.demo.entity.Item;
import com.example.demo.entity.ItemDetail;
import com.example.demo.mapper.CstDAO;
import com.example.demo.repo.ItemRepo;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CstService {
    private static final Executor executor = ExecutorBuilder.create().setCorePoolSize(10).setMaxPoolSize(15).build();

    private static final AtomicLong count = new AtomicLong();
    @Resource
    private ItemRepo itemRepo;

    @Resource
    private RedisTemplate<String, String> redisTemplate;

    @Resource
    private JdbcClient jdbcClient;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private CstDAO cstDAO;

    @Resource
    private CstService cstService;

    public void saveAll(List<Item> items) {
        itemRepo.saveAll(items);
    }

    public void prepareCopy() {
        List<String> a = new ArrayList<>();
        redisTemplate.delete("copyList");
        long count = jdbcClient.sql("select count(*) from item").query(Long.class).single();
        Long minId = jdbcClient.sql("select id from item order by id limit 1").query(Long.class).single();
        long maxId = jdbcClient.sql("select id from item order by id desc limit 1").query(Long.class).single();
        for (int i = 0; i < count / 5000 + 1; i++) {
            List<Long> itemList = jdbcClient.sql("select id from item where id>=? order by id limit 5000").param(minId).query(Long.class).list();
            if (CollectionUtil.isEmpty(itemList)) {
                break;
            }
            var id = itemList.getLast();
            a.add(minId + "%" + Math.min(maxId, id));
            minId = id + 1;
        }
        redisTemplate.opsForList().rightPushAll("copyList", a);
//        Lists.partition()
    }

    public void prepareTest(){
        long count = jdbcClient.sql("select count(*) from item").query(Long.class).single();
        for (int i = 0; i < count / 1000 + 1; i++) {
            List<String> tmpList = jdbcClient.sql("select id from item  order by id limit 1000 offset ?").param(i*100000).query(String.class).list();
            if (tmpList.isEmpty()) {
                break;
            }
            redisTemplate.opsForSet().add("set2",tmpList.toArray(new String[0]));
        }

    }

    @Transactional
    public void copy() throws InterruptedException {
        var startTime = System.currentTimeMillis();
        prepareCopy();
        log.info("prepare : {}s", (System.currentTimeMillis() - startTime) / 1000);
        var tmp = redisTemplate.opsForList().range("copyList", 0, -1);
        assert tmp != null;
        var rangeList = tmp.stream().map(it -> it.split("%", 2)).toList();
        CountDownLatch countDownLatch = new CountDownLatch(rangeList.size());
        for (int i = 0; i < rangeList.size(); i++) {
            int finalI = i;
            CompletableFuture.runAsync(() -> {
                var b = rangeList.get(finalI);
                List<Long> itemList = getItemIdList(b);
                if (CollectionUtil.isEmpty(itemList)) {
                    countDownLatch.countDown();
                    return;
                }
                List<CstVO> list = itemList.stream().map(it -> new CstVO(IdUtil.getSnowflakeNextId(), it,1L)).toList();
                Map<Long,CstVO> map = list.stream().collect(Collectors.toMap(CstVO::getItemId, Function.identity()));
                cstDAO.copy(list,Long.parseLong(b[0]),Long.parseLong(b[1]));
                List<ItemDetail> itemDetails = jdbcClient.sql("select id,item_id from item_detail where item_id >= :startId and item_id<=:endId order by id ").param("startId", b[0], Types.BIGINT)
                        .param("endId", b[1], Types.BIGINT)
                        .query(ItemDetail.class)
                        .list();
                List<CstVO> list1 = itemDetails.stream().map(it->new CstVO(it.getId(),map.get(it.getItemId()).getId(),IdUtil.getSnowflakeNextId())).toList();
                cstDAO.copy1(list1,Long.parseLong(b[0]),Long.parseLong(b[1]));
                countDownLatch.countDown();
            },executor);

        }
        countDownLatch.await();
        log.info("cost : {}s", (System.currentTimeMillis() - startTime) / 1000);

    }


    public List<Item> getItemList(String[] b) {
        return jdbcClient.sql("select data_version,pac_id,org_code from item where id>=:startId and id<=:endId order by id ")
                .param("startId", b[0], Types.BIGINT)
                .param("endId", b[1], Types.BIGINT)
                .query(Item.class)
                .list();
    }

    public List<Long> getItemIdList(String[] b){
        return jdbcClient.sql("select id from item where id>=:startId and id<=:endId order by id ")
                .param("startId", b[0], Types.BIGINT)
                .param("endId", b[1], Types.BIGINT)
                .query(Long.class)
                .list();
    }

    public void insert(List<Item> itemList) {
        String sql = """
                insert into item values(?,?,?,?);
                """;
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
    }


}
