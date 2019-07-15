package com.zhiboclub.ycweb.domain;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class FansInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String userName;
    private String userAvatar;
    private Integer taoQi;
    private Boolean aPass;
    private Boolean vip;
    private Timestamp createAt;
    private Timestamp updateAt;

}
