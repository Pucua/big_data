package com.atguigu.financial.lease.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwdAuditCancelBean {
    // 授信申请 ID
    String id;

    // 业务方向
    String leaseOrganization;

    // 申请人 ID
    String businessPartnerId;

    // 行业 ID
    String industryId;

    // 业务经办 ID
    String salesmanId;

    // 信审经办 ID
    String auditManId;

    // 申请授信金额
    BigDecimal applyAmount;

    // 取消时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String cancelTime;

}
