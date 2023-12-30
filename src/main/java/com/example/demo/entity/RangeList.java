package com.example.demo.entity;

import java.util.AbstractList;
import java.util.List;

public  class RangeList extends AbstractList<List<Long>> {
    final long start;
    final long end;

    final long step;

    long endIndex;

    long batch;

    public RangeList(long start,long end, long batch) {
        this.start = start;
        this.end = end;
        this.step = (end-start)/batch;
        this.endIndex = -1;
        this.batch = batch;
    }

    public List<Long> get(int index) {
        long startIdx = Math.max(this.start + index * this.step,endIndex);
        long endIdx = Math.min(startIdx + this.step,end);
        endIndex = endIdx+1;
        return List.of(startIdx,endIdx);
    }

    public int size() {
        return (int) this.batch;
    }
}