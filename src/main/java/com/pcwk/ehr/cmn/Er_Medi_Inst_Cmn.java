package com.pcwk.ehr.cmn;

public class Er_Medi_Inst_Cmn {

	public static String gu_Nm_From_Gu_Code(String gu_Code) {

		switch (gu_Code) {
		case "111123":
			return "종로구";
		case "111121":
			return "중구";
		case "111131":
			return "용산구";
		case "111181":
			return "은평구";
		case "111201":
			return "마포구";
		case "111142":
			return "성동구";
		case "111141":
			return "광진구";
		case "111151":
			return "중랑구";
		case "111161":
			return "성북구";
		case "111291":
			return "강북구";
		case "111171":
			return "도봉구";
		case "111311":
			return "노원구";
		case "111301":
			return "양천구";
		case "111212":
			return "강서구";
		case "111221":
			return "구로구";
		case "111281":
			return "금천구";
		case "111241":
			return "동작구";
		case "111251":
			return "관악구";
		case "111262":
			return "서초구";
		case "111261":
			return "강남구";
		case "111273":
			return "송파구";
		case "111274":
			return "강동구";
		case "111191":
			return "서대문구";
		case "111152":
			return "동대문구";
		case "111231":
			return "영등포구";
		default:
			return "전체";
		}

	}

}
