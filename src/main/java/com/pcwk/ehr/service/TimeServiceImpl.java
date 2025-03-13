package com.pcwk.ehr.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pcwk.ehr.mapper.TimeMapper;

@Service
public class TimeServiceImpl implements TimeService {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	TimeMapper timeMapper;

	public TimeServiceImpl() {
		super();
		log.info("┌─────────────────────┐");
		log.info("│  TimeServiceImpl()  │");
		log.info("└─────────────────────┘");
	}

	@Override
	public String getNewDateTime() {
		return timeMapper.getNewDateTime();
	}

	@Override
	public String getDateTime() {
		return timeMapper.getDateTime();
	}

}
