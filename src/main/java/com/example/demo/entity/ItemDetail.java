package com.example.demo.entity;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class ItemDetail {
    private Long id;
    private Long itemId;
    private String sysDesc;
    private BigDecimal itemCost;
}
