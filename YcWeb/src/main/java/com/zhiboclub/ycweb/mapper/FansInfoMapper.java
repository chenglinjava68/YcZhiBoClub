package com.zhiboclub.ycweb.mapper;

import com.zhiboclub.ycweb.domain.FansInfo;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

public interface FansInfoMapper {


    @Select("select * from fansinfo where userid=#{userid}")
    @Results({
            @Result(column ="userid",property = "userId"),
            @Result(column ="username",property = "userName"),
            @Result(column ="useravatar",property = "userAvatar"),
            @Result(column ="taoQiHi",property = "taoQiHi"),
            @Result(column ="apassuser",property = "aPassUser"),
            @Result(column ="vipuser",property = "vipUser"),
            @Result(column ="createtime",property = "createTime"),
            @Result(column ="updatetime",property = "updateTime")
    })
    public FansInfo getByid(String userid);
}
