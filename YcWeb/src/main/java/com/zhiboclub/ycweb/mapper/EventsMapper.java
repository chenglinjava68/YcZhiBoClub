package com.zhiboclub.ycweb.mapper;

import com.zhiboclub.ycweb.domain.Events;
import org.apache.ibatis.annotations.*;

import java.sql.Timestamp;
import java.util.List;

@Mapper
public interface EventsMapper {

    /**
     * 查询当前直播id，不同消息类型的总数
     *
     * @param liveid 直播id
     * @param type   消息类型
     * @return 总数
     */
    @Select("SELECT count(1) from events WHERE \"liveId\"=#{liveid} and type=#{type}")
    Integer getEventsByLiveIdAndType(String liveid, String type);

    @Select("SELECT count(DISTINCT \"userId\") from events WHERE \"liveId\"=#{liveid} and type=#{type}")
    Integer getEventsByLiveIdAndTypeDistinct(String liveid, String type);

    /**
     * 查询当前直播id下，当type=join时最后一条join的pv和uv信息
     *
     * @param liveid
     * @return
     */
    @Select("SELECT body FROM events WHERE \"liveId\"=#{liveid} and type='join' ORDER BY \"createdAt\" desc LIMIT 1")
    String getEventsBody2CountPvUv(String liveid);


    @Select("SELECT \"createdAt\",\"startTime\" from events where \"liveId\"=#{liveid} ORDER BY \"createdAt\" desc limit 1;")
    @Results({
            @Result(column = "createdAt", property = "createTime"),
            @Result(column = "updatedAt", property = "updateTime")
    })
    Events getEventsTimeRange(String liveid);


    /**
     * 查询我的直播时间段期间，符合条件的用户行为记录表
     * @param liveid
     * @param starttime
     * @param endtime
     * @return
     */

    @Select("select * from (select  \"liveId\",\"anchorId\",\"userId\",title,\"startTime\",\"createdAt\" from events WHERE \"userId\""+
        " in (select a.\"userId\" from "+
        "(select distinct \"liveId\",\"userId\" from events where \"liveId\"=${liveid})a\n"+
        "join (select distinct \"liveId\",\"userId\" from events where \"liveId\" !=${liveid} and \"createdAt\" > '${starttime}' AND \"createdAt\" < '${endtime}')b"+
        " ON "+
        "a.\"userId\"=b.\"userId\")"+
        ")c ORDER BY \"userId\",\"createdAt\"")
    @Results({
            @Result(column = "liveId", property = "liveId"),
            @Result(column = "anchorId", property = "anchorId"),
            @Result(column = "userId", property = "user" ,one = @One(select = "com.zhiboclub.ycweb.mapper.FansInfoMapper.getByid")),
            @Result(column = "title", property = "title"),
            @Result(column = "startTime", property = "startTime"),
//            @Result(column = "type", property = "type"),
//            @Result(column = "typecode", property = "typeCode"),
//            @Result(column = "body", property = "body"),
            @Result(column = "createdAt", property = "createTime"),
            @Result(column = "updateAt", property = "updateTime")
    })
    List<Events> getEventsTimeUsers(String liveid, Timestamp starttime, Timestamp endtime);
}