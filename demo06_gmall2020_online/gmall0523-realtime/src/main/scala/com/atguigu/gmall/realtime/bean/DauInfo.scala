package com.atguigu.gmall.realtime.bean

/**
 * @author : Pucua
 * @date : 2023-10-11 11:42
 * @Desc : 封装日活数据的样例类
 **/
case class DauInfo(
                    mid:String,//设备 id
                    uid:String,//用户 id
                    ar:String,//地区
                    ch:String,//渠道
                    vc:String,//版本
                    var dt:String,//日期
                    var hr:String,//小时
                    var mi:String,//分钟
                    ts:Long //时
                  ){}
