package com.example.demo.mapper;

import com.example.demo.entity.CstVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface CstDAO {
    void copy(@Param("vo") List<CstVO> list, @Param("min_id") Long minId, @Param("max_id") Long maxId);

    void copy1(@Param("vo") List<CstVO> list, @Param("min_id") Long minId, @Param("max_id") Long maxId);
}
