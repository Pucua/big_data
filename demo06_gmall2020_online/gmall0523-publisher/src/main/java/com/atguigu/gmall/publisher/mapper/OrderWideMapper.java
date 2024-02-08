package com.atguigu.gmall.publisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author : Pucua
 * @date : 2023-10-16 15:07
 * @Desc :订单宽表进行操作的接口
 **/
public interface OrderWideMapper {
    //查询当日交易额总数
    public BigDecimal selectOrderAmountTotal(String date);
    //查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

    List<Map> selectOrderAmountHour(String date);
}
