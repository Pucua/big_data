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
public class DwdAuditReplyBean {
    // 批复 ID
    String id;

    // 授信申请 ID
    String creditFacilityId;

    // 批复时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String replyTime;

    // 批复金额
    BigDecimal replyAmount;

    // 还款利率
    BigDecimal irr;

    // 还款期数
    Long period;

}
