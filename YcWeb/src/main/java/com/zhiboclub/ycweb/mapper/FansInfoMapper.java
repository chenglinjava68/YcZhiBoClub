package com.zhiboclub.ycweb.mapper;

import com.zhiboclub.ycweb.domain.FansInfo;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

public interface FansInfoMapper {


    @Select("select * from fansinfo where \"userId\"=#{userid}")
    @Results({
            @Result(column ="userId",property = "userId"),
            @Result(column ="userName",property = "userName"),
            @Result(column ="userAvatar",property = "userAvatar"),
            @Result(column ="taoqi",property = "taoQi"),
            @Result(column ="apass",property = "aPass"),
            @Result(column ="vip",property = "vip"),
            @Result(column ="createdAt",property = "createAt"),
            @Result(column ="updatedAt",property = "updateAt")
    })
    FansInfo getByid(String userid);
}
