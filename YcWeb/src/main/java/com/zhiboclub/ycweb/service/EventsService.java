package com.zhiboclub.ycweb.service;

import com.zhiboclub.ycweb.domain.Events;

import java.sql.Timestamp;
import java.util.List;

public interface EventsService {
    Integer getEventsByLiveIdAndType(String liveid, String type);

    Integer getEventsByLiveIdAndTypeDistinct(String liveid, String type);

    String getEventsBody2CountPvUv(String liveid);

    Events getEventsTimeRange(String liveid);

    List<Events> getEventsTimeUsers(String liveid, Timestamp starttime, Timestamp endtime);

}
