package com.pcwk.ehr.time;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.pcwk.ehr.mapper.TimeMapper;

@SpringBootTest
class TimeMapperJUnit {

	final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	TimeMapper timeMapper;

	@BeforeEach
	void setUp() throws Exception {
		log.info("┌───────────────┐");
		log.info("│    setUp()    │");
		log.info("└───────────────┘");
	}

	@AfterEach
	void tearDown() throws Exception {
		log.info("┌────────────────┐");
		log.info("│   tearDown()   │");
		log.info("└────────────────┘");
	}

	@Test
	void getDateTime() {
		log.info("┌───────────────────┐");
		log.info("│   getDateTime()   │");
		log.info("└───────────────────┘");

		String dateTime = timeMapper.getDateTime();
		log.info("dateTime : {}", dateTime);

		assertNotNull(dateTime);

	}

	@Test
	void getNewDateTime() {
		log.info("┌──────────────────────┐");
		log.info("│   getNewDateTime()   │");
		log.info("└──────────────────────┘");

		String new_DateTime = timeMapper.getNewDateTime();
		log.info("new_DateTime : {}", new_DateTime);

		assertNotNull(new_DateTime);

	}

	@Test
	void beans() {
		log.info("┌─────────────┐");
		log.info("│   beans()   │");
		log.info("└─────────────┘");

		log.info("TimeMaaper : {}", timeMapper);

		assertNotNull(timeMapper);

	}

}
