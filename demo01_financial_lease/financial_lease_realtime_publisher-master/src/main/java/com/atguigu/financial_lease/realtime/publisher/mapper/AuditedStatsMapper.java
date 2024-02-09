package com.atguigu.financial_lease.realtime.publisher.mapper;

import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsDepartAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsLeaseOrgAuditApproveBean;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface AuditedStatsMapper {

    @Select("select lease_organization,\n" +
            "       sum(apply_amount) apply_amount,\n" +
            "       sum(reply_amount) reply_amount\n" +
            "from dws_audit_industry_lease_organization_salesman_approval_win\n" +
            "where cur_date = #{date}\n" +
            "group by lease_organization")
    public List<AuditedStatsLeaseOrgAuditApproveBean> selectAuditedLeaseOrgAuditApproveStats(@Param("date") String date);

    @Select("select department3_id,\n" +
            "       department3_name,\n" +
            "       department2_id,\n" +
            "       department2_name,\n" +
            "       department1_id,\n" +
            "       department1_name,\n" +
            "       sum(reply_amount) reply_amount\n" +
            "from dws_audit_industry_lease_organization_salesman_approval_win\n" +
            "where cur_date = #{date}\n" +
            "group by department3_id,\n" +
            "         department3_name,\n" +
            "         department2_id,\n" +
            "         department2_name,\n" +
            "         department1_id,\n" +
            "         department1_name;")
    public List<AuditedStatsDepartAuditApproveBean> selectAuditedDepartAuditApproveStats(@Param("date") String date);
}
