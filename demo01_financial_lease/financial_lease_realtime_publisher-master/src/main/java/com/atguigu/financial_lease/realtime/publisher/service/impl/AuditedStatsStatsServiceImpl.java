package com.atguigu.financial_lease.realtime.publisher.service.impl;

import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsDepartAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsLeaseOrgAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.mapper.AuditedStatsMapper;
import com.atguigu.financial_lease.realtime.publisher.service.AuditedStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AuditedStatsStatsServiceImpl implements AuditedStatsService {

    @Autowired
    private AuditedStatsMapper auditedStatsMapper;

    @Override
    public List<AuditedStatsLeaseOrgAuditApproveBean> getAuditedLeaseOrgAuditApproveStats(String date) {
        return auditedStatsMapper.selectAuditedLeaseOrgAuditApproveStats(date);
    }

    @Override
    public List<AuditedStatsDepartAuditApproveBean> getAuditedDepartAuditApproveStats(String date) {
        return auditedStatsMapper.selectAuditedDepartAuditApproveStats(date);
    }
}
