package com.zhiboclub.ycweb.service.impl;

import com.zhiboclub.ycweb.domain.Events;
import com.zhiboclub.ycweb.mapper.EventsMapper;
import com.zhiboclub.ycweb.service.EventsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;

@Service
public class EventsServiceImpl implements EventsService {

    @Autowired
    private EventsMapper eventsMapper;

    @Override
    public Integer getEventsByLiveIdAndType(String liveid, String type) {
        return eventsMapper.getEventsByLiveIdAndType(liveid,type);
    }

    @Override
    public Integer getEventsByLiveIdAndTypeDistinct(String liveid, String type) {
        return eventsMapper.getEventsByLiveIdAndTypeDistinct(liveid,type);
    }

    @Override
    public String getEventsBody2CountPvUv(String liveid) {
        return eventsMapper.getEventsBody2CountPvUv(liveid);
    }

    @Override
    public Events getEventsTimeRange(String liveid) {
        return eventsMapper.getEventsTimeRange(liveid);
    }

    @Override
    public List<Events> getEventsTimeUsers(String liveid, Timestamp starttime, Timestamp endtime) {
        return eventsMapper.getEventsTimeUsers(liveid,starttime,endtime);
    }
}
