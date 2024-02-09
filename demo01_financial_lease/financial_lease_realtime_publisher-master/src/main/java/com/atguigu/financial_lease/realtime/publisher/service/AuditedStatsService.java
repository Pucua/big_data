package com.atguigu.financial_lease.realtime.publisher.service;

import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsDepartAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsLeaseOrgAuditApproveBean;

import java.util.List;

public interface AuditedStatsService {

    public List<AuditedStatsLeaseOrgAuditApproveBean> getAuditedLeaseOrgAuditApproveStats(String date);

    public List<AuditedStatsDepartAuditApproveBean> getAuditedDepartAuditApproveStats(String date);
}
