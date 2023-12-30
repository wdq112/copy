package com.example.demo.repo;

import com.example.demo.entity.Item;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ItemRepo extends JpaRepository<Item,Long> {

    @Query("select t from Item t where t.id>:startId and t.id<:endId ")
    List<Item> findByIds(@Param("startId") Long startId,@Param("endId") Long endId);

    @Query("select t from Item t where t.id>=:startId")
    List<Item> findByIds(@Param("startId") Long startId);
}
