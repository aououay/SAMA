package com.pcwk.ehr.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface TimeMapper {

	public String getNewDateTime();

	@Select("SELECT SYSDATE FROM dual")
	public String getDateTime();

}