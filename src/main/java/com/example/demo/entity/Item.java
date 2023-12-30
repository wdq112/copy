package com.example.demo.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "item")
public class Item {
    @Id
    private Long id;
    private Long dataVersion;
    private Long pacId;
    private String orgCode;
}
