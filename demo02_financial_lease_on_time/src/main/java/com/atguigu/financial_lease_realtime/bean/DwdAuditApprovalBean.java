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
public class DwdAuditApprovalBean {
    // 授信申请 ID
    String id;

    // 业务方向
    String leaseOrganization;

    // 申请人 ID
    String businessPartnerId;

    // 行业 ID
    String industryId;

    // 批复 ID
    String replyId;

    // 业务经办 ID
    String salesmanId;

    // 信审经办 ID
    String auditManId;

    // 申请授信金额
    BigDecimal applyAmount;

    // 批复金额
    BigDecimal replyAmount;

    // 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String approveTime;

    // 批复时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String replyTime;

    // 还款利率
    BigDecimal irr;

    // 还款期数
    Long period;

}
