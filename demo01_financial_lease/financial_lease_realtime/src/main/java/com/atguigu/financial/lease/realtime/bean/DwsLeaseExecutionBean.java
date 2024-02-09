package com.atguigu.financial.lease.realtime.bean;

import com.atguigu.financial.lease.realtime.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsLeaseExecutionBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 申请项目数
    Long applyCount;

    // 申请金额
    BigDecimal applyAmount;

    // 批复金额
    BigDecimal replyAmount;

    // 授信金额
    BigDecimal creditAmount;

    // 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    @TransientSink
    String executionTime;

    public Long getTs() {
        return DateFormatUtil.toTs(executionTime);
    }
}