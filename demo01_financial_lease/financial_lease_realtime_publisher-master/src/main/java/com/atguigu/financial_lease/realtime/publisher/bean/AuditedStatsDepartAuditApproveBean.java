package com.atguigu.financial_lease.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class AuditedStatsDepartAuditApproveBean {

    // 三级部门 ID
    String department3Id;

    // 三级部门名称
    String department3Name;

    // 二级部门 ID
    String department2Id;

    // 二级部门名称
    String department2Name;

    // 一级部门 ID
    String department1Id;

    // 一级部门名称
    String department1Name;

    // 批复金额
    BigDecimal replyAmount;
}
