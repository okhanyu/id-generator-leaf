package com.sankuai.inf.leaf.server.controller;

import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.server.exception.LeafServerException;
import com.sankuai.inf.leaf.server.exception.NoKeyException;
import com.sankuai.inf.leaf.server.service.SegmentService;
import com.sankuai.inf.leaf.server.service.SnowflakeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class LeafController {
    private Logger logger = LoggerFactory.getLogger(LeafController.class);

    private static final String CONST_KEY = "online";

    @Autowired
    private SegmentService segmentService;
    @Autowired
    private SnowflakeService snowflakeService;

    @RequestMapping(value = "/api/segment/get/{key}")
    public String getSegmentId(@PathVariable("key") String key) {
        return get(key, segmentService.getId(key));
    }

    @RequestMapping(value = "/api/snowflake/get/{key}")
    public String getSnowflakeId(@PathVariable("key") String key) {
        return get(key, snowflakeService.getId(key));
    }

    @RequestMapping(value = "/api/snowflake/get")
    public String getSnowflakeId() {
        return get(CONST_KEY, snowflakeService.getId(CONST_KEY));
    }

    @RequestMapping(value = "/api/snowflake/get/batch/{count}")
    public List<String> getSnowflakeIdBatch(@PathVariable("count") Integer count) {
        count = count == null || count == 0 ? 1 : count;
        List<String> list = new ArrayList();
        for (int i = 0; i < count; i++) {
            try {
                list.add(get(CONST_KEY, snowflakeService.getId(CONST_KEY)));
            } catch (Exception e) {
                logger.error("id create error", e);
            }
        }
        if (list.isEmpty()) {
            throw new LeafServerException("all id create error");
        }
        return list;
    }

    private String get(@PathVariable("key") String key, Result id) {
        Result result;
        if (key == null || key.isEmpty()) {
            throw new NoKeyException();
        }
        result = id;
        if (result.getStatus().equals(Status.EXCEPTION)) {
            throw new LeafServerException(result.toString());
        }
        return String.valueOf(result.getId());
    }
}
