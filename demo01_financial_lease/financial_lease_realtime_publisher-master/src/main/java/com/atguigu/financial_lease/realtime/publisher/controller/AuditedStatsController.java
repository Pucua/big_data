package com.atguigu.financial_lease.realtime.publisher.controller;

import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsDepartAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.bean.AuditedStatsLeaseOrgAuditApproveBean;
import com.atguigu.financial_lease.realtime.publisher.service.AuditedStatsService;
import com.atguigu.financial_lease.realtime.publisher.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/financial_lease/realtime")
public class AuditedStatsController {

    @Autowired
    private AuditedStatsService auditedStatsService;

    @RequestMapping("/lease_org/approval/stats")
    public String getLeaseOrgAuditApproveStats(@RequestParam(value = "date", defaultValue = "1") String date) {
        if (date.equals("1")) {
            date = DateUtil.now();
        }

        List<AuditedStatsLeaseOrgAuditApproveBean> leaseOrgAuditApproveStats = auditedStatsService.getAuditedLeaseOrgAuditApproveStats(date);

        StringBuilder categories = new StringBuilder("\"");
        StringBuilder applyAmounts = new StringBuilder("\"");
        StringBuilder replyAmounts = new StringBuilder("\"");

        for (int i = 0; i < leaseOrgAuditApproveStats.size(); i++) {
            AuditedStatsLeaseOrgAuditApproveBean auditedStatsLeaseOrgAuditApproveBean = leaseOrgAuditApproveStats.get(i);

            categories.append(auditedStatsLeaseOrgAuditApproveBean.getLeaseOrganization());
            applyAmounts.append(auditedStatsLeaseOrgAuditApproveBean.getApplyAmount());
            replyAmounts.append(auditedStatsLeaseOrgAuditApproveBean.getReplyAmount());

            if (i < leaseOrgAuditApproveStats.size() - 1) {
                categories.append("\",\"");
                applyAmounts.append("\",\"");
                replyAmounts.append("\",\"");
            } else {
                categories.append("\"");
                applyAmounts.append("\"");
                replyAmounts.append("\"");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\n" +
                "      " + categories + "\n" +
                "    ],\n" +
                "    \"series\": [\n" +
                "      {\n" +
                "        \"name\": \"申请项目金额\",\n" +
                "        \"data\": [\n" +
                "          " + applyAmounts + "\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"批复项目金额\",\n" +
                "        \"data\": [\n" +
                "          " + replyAmounts + "\n" +
                "        ]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/department/approval/stats")
    public String getDepApprovalStats(@RequestParam(value = "date", defaultValue = "1") String date) {
        if (date.equals("1")) {
            date = DateUtil.now();
        }

        List<AuditedStatsDepartAuditApproveBean> auditedDepartAuditApproveStats = auditedStatsService.getAuditedDepartAuditApproveStats(date);

        StringBuilder dataBuilder = new StringBuilder();

        for (int i = 0; i < auditedDepartAuditApproveStats.size(); i++) {
            AuditedStatsDepartAuditApproveBean bean = auditedDepartAuditApproveStats.get(i);
            String tmp = "{\n" +
                    "      \"name\": \"三级部门: " + bean.getDepartment3Name() + ", 二级部门: " + bean.getDepartment2Name() + ", 一级部门: " + bean.getDepartment1Name() + "\",\n" +
                    "      \"value\": " + bean.getReplyAmount() + "\n" +
                    "    }";
            dataBuilder.append(tmp);

            if (i < auditedDepartAuditApproveStats.size() - 1) {
                dataBuilder.append(",\n");
            } else {
                dataBuilder.append("\n");
            }
        }

        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": [\n" +
                "    " + dataBuilder + "\n" +
                "  ]\n" +
                "}";
    }
}
