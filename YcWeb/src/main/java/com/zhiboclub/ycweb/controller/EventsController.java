package com.zhiboclub.ycweb.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhiboclub.ycweb.domain.Events;
import com.zhiboclub.ycweb.service.EventsService;
import com.zhiboclub.ycweb.utils.JsonData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
public class EventsController {

    @Autowired
    private EventsService eventsService;

    /**
     * 查询当前直播id，不同类型的统计
     * @param liveid 直播id
     * @param type 消息类型
     *         -- 取值范围
     *            -- 101	follow
     *            -- 10004	biz
     *            -- 10005	join
     *            -- 101	txt
     *            -- 10003	shares
     *            -- 10008	share_goods_list
     *            -- 10010	trade_show
     * @return 总数
     *
     * e.g. http://localhost:8000/get_liveid_type_count?liveid=230338877811&type=join
     */
    @GetMapping("/get_liveid_type_count")
    public Object getEventsByLiveIdAndType(String liveid,String type){
        return JsonData.buildSuccess(eventsService.getEventsByLiveIdAndType(liveid,type));
    }

    /**
     * 获取当前直播id，不同类型的去重统计
     * @param liveid 直播id
     * @param type 消息类型
     *         -- 取值范围
     *            -- 101	follow
     *            -- 10004	biz
     *            -- 10005	join
     *            -- 101	txt
     *            -- 10003	shares
     *            -- 10008	share_goods_list
     *            -- 10010	trade_show
     * @return 总数
     *
     * e.g. http://localhost:8000/get_liveid_type_distinct_count?liveid=230338877811&type=txt
     */
    @GetMapping("/get_liveid_type_distinct_count")
    public Object getEventsByLiveIdAndTypeDistinct(String liveid,String type){
        return JsonData.buildSuccess(eventsService.getEventsByLiveIdAndTypeDistinct(liveid,type));
    }

    /**
     * 获取uv和pv等
     * @param liveid
     * @return
     * http://localhost:8000/get_liveid_count_pvuv?liveid=230338877811
     */

    @GetMapping("/get_liveid_count_pvuv")
    Object getEventsBody2CountPvUv(String liveid){
        String body = eventsService.getEventsBody2CountPvUv(liveid);
        JSONObject json = JSONObject.parseObject(body);
        JSONObject join = JSONObject.parseObject(json.getJSONObject("join").toString());
        Map<String,String> map = new HashMap<>();
        map.put("pageViewCount",join.get("pageViewCount").toString());
        map.put("totalCount",join.get("totalCount").toString());
        map.put("onlineCount",join.get("onlineCount").toString());
        return JsonData.buildSuccess(JSONObject.toJSONString(map));
    }

    /**
     * 直播时长（h）
     * @param liveid
     * @return 直播时间
     * http://localhost:8000/get_liveid_time?liveid=230338877811
     */

    @GetMapping("/get_liveid_time")
    Object getEventsTime(String liveid){
        Events events = eventsService.getEventsTimeRange(liveid);
        float timerange_min = (events.getCreateTime().getTime() - events.getStartTime().getTime())/(1000*60);
        return JsonData.buildSuccess(timerange_min/60);
    }

    /**
     * 直播时间段
     * @param liveid
     * @return
     * http://localhost:8000/get_liveid_timerange?liveid=230338877811
     */
    @GetMapping("/get_liveid_timerange")
    Object getEventsTimeRange(String liveid){
        Events events = eventsService.getEventsTimeRange(liveid);
        Map<String,Long> map = new HashMap<>();
        map.put("startTime", events.getStartTime().getTime());
        map.put("endTime",events.getCreateTime().getTime());
        return JsonData.buildSuccess(map);
    }

    /**
     * 直播时间段用户流向情况
     * @param liveid
     * @return
     * http://localhost:8000/get_liveid_timerange_user?liveid=230338877811
     */
    @GetMapping("/get_liveid_timerange_user")
    Object getEventsTimeUsers(String liveid){
        Events events = eventsService.getEventsTimeRange(liveid);
        List<Events> eventsall = eventsService.getEventsTimeUsers(liveid,events.getStartTime(),events.getCreateTime());

        return JsonData.buildSuccess(eventsall.size());
    }
}
