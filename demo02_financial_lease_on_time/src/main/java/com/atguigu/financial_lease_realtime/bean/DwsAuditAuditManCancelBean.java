package com.atguigu.financial_lease_realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsAuditAuditManCancelBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 信审经办 ID
    String auditManId;

    // 信审经办姓名
    String auditManName;

    // 申请项目数
    Long applyCount;

    // 申请金额
    BigDecimal applyAmount;

    // 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    @TransientSink
    String cancelTime;

    public Long getTs() {
        return DateFormatUtil.toTs(cancelTime);
    }

}
