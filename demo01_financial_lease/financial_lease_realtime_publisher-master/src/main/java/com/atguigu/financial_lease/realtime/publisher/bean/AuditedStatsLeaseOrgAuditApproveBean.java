package com.atguigu.financial_lease.realtime.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class AuditedStatsLeaseOrgAuditApproveBean {

    // 业务方向
    String leaseOrganization;

    // 审批通过项目申请金额
    Long applyAmount;

    // 审批通过项目批复金额
    Long replyAmount;
}
