package com.pcwk.ehr.board.controller;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;
import com.pcwk.ehr.board.domain.BoardVO;
import com.pcwk.ehr.board.service.BoardService;
import com.pcwk.ehr.cmn.MessageVO;
import com.pcwk.ehr.cmn.SearchBoardVO;
import com.pcwk.ehr.cmn.SearchVO;
import com.pcwk.ehr.cmn.StringUtil;
import com.pcwk.ehr.file.domain.FileVO;
import com.pcwk.ehr.file.service.FileService;
import com.pcwk.ehr.reply.domain.ReplyVO;
import com.pcwk.ehr.reply.service.ReplyService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

@RequestMapping("board")
@Controller
public class BoardController {

	final Logger log = LoggerFactory.getLogger(getClass());
	final String fileFullPath = "D:\\upload";

	@Autowired
	BoardService boardService;

	@Autowired
	ReplyService replyService;

	@Autowired
	FileService fileService;

	public BoardController() {
		log.info("┌──────────────────────────┐");
		log.info("│ BoardController()        │");
		log.info("└──────────────────────────┘");
	}

	@GetMapping("/doRetrieve.do")
	public String doRetrieve(HttpServletRequest req, Model model) {
		log.info("┌──────────────────────────┐");
		log.info("│ doRetrieve()             │");
		log.info("└──────────────────────────┘");

		SearchBoardVO search = new SearchBoardVO();

		String pageNoString = StringUtil.nvl(req.getParameter("pageNo"), "1");
		String pageSizeString = StringUtil.nvl(req.getParameter("pageSize"), "10");

		int pageNo = Integer.parseInt(pageNoString);
		int pageSize = Integer.parseInt(pageSizeString);
		log.info("pageNo:{}", pageNo);
		log.info("pageSize:{}", pageSize);

		search.setPageNo(pageNo);
		search.setPageSize(pageSize);

		// 10(공지사항),20(자유게시판),30(재난안전게시판)
		String div = StringUtil.nvl(req.getParameter("div"), "10");
		log.info("div:{}", div);

		search.setOptionDiv(div);

		// 데이터가 null이면 , ""
		String searchDiv = StringUtil.nvl(req.getParameter("searchDiv"), "");
		String searchWord = StringUtil.nvl(req.getParameter("searchWord"), "");
		log.info("searchDiv:{}", searchDiv);
		log.info("searchWord:{}", searchWord);

		search.setSearchDiv(searchDiv);
		search.setSearchWord(searchWord);

		log.info("search:{}", search);

		List<BoardVO> boardList = boardService.doRetrieve(search);

		for (BoardVO vo : boardList) {
			log.info("vo:{}", vo);
		}

		// 총 글수
		int totalCnt = 0;

		if (null != boardList && boardList.size() > 0) {
			BoardVO vo = boardList.get(0);

			totalCnt = vo.getTotalCnt();
		}

		model.addAttribute("totalCnt", totalCnt);
		model.addAttribute("boardList", boardList);
		model.addAttribute("board_div", div);
		model.addAttribute("search", search);

		return "board/board_list";
	}

	@GetMapping("/board_reg_index.do")
	public String boardRegIndex(@RequestParam(name = "div", defaultValue = "10") String div, Model model) {
		log.info("┌──────────────────────────┐");
		log.info("│ boardRegIndex()          │");
		log.info("└──────────────────────────┘");

		log.info("div:" + div);

		model.addAttribute("board_div", div);

		return "board/board_reg";
	}

	@PostMapping(value = "/doSave.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doSave(BoardVO boardVO, @RequestParam(value = "file", required = false) MultipartFile file)
			throws SQLException {
		String jsonString = "";
		log.info("┌──────────────────────────┐");
		log.info("│ doSave()                 │");
		log.info("└──────────────────────────┘");
		log.info("boardVO: {}", boardVO);

		// 게시글 저장 처리
		int flag = this.boardService.doSave(boardVO);
		log.info("flag: {}", flag);

		String message = "";
		if (1 == flag) {
			message = "게시글이 등록 되었습니다.";

			// 게시글 저장 후 boardId 가져오기
			int boardId = boardService.getBoardSequence();
			log.info("boardId:{}", boardId);

			// 파일 업로드 처리
			if (file != null && !file.isEmpty()) {
				FileVO vo = new FileVO();
				String orgFileName = file.getOriginalFilename();
				long fileSize = file.getSize();
				String saveFileName = StringUtil.getPK("yyyyMMddHHmmss");
				String ext = StringUtil.getExt(orgFileName);

				try {
					// 파일 저장 정보 설정
					vo.setOrgFileName(orgFileName);
					vo.setFileSize(fileSize);
					vo.setSavePath(fileFullPath); // 파일 저장 경로
					vo.setSaveFileName(saveFileName + "." + ext);
					vo.setExtension(ext);
					vo.setBoardId(boardId); // 게시글 ID 설정

					log.info("orgFileName:{}", orgFileName);
					log.info("fileSize:{}", fileSize);
					log.info("fileFullPath:{}", fileFullPath);
					log.info("saveFileName:{}", saveFileName + "." + ext);
					log.info("ext:{}", ext);
					log.info("boardId:{}", vo.getBoardId());

					// 파일을 지정한 경로에 저장
					file.transferTo(new File(fileFullPath + File.separator + vo.getSaveFileName()));

					// 파일 저장 후, DB에 파일 정보를 추가 저장
					this.fileService.saveFile(vo); // 파일 정보를 DB에 저장하는 서비스 호출

					// message += " 파일이 업로드 되었습니다.";
				} catch (IllegalStateException | IOException e) {
					message = "게시글 등록 실패: 파일 업로드 중 오류 발생 - " + e.getMessage();
				}
			}

		} else {
			message = "게시글 등록 실패.";
		}

		// 결과 메시지와 플래그를 담아서 클라이언트로 반환
		MessageVO messageVO = new MessageVO(flag, message);
		jsonString = new Gson().toJson(messageVO);
		log.info("jsonString:\n{}", jsonString);

		return jsonString;
	}

	@GetMapping("/doSelectOne.do")
	public String doSelectOne(@RequestParam(name = "div", defaultValue = "10") String div, Model model,
			@RequestParam(name = "boardId", defaultValue = "0") int boardId, HttpSession session)
			throws NullPointerException, SQLException {

		log.info("┌──────────────────────────┐");
		log.info("│ doSelectOne()            │");
		log.info("└──────────────────────────┘");

		BoardVO boardVO = new BoardVO();
		boardVO.setDiv(div);
		boardVO.setBoardId(boardId);

		log.info("div:{}", boardVO.getDiv());
		log.info("boardId:{}", boardVO.getBoardId());

		BoardVO outVO = boardService.doSelectOne(boardVO);
		List<ReplyVO> replyList = replyService.getReply(boardVO);
		FileVO fileVO = fileService.getFile(boardVO);

		log.info("outVO:{}", outVO);
		for (ReplyVO reply : replyList) {
			log.info("reply:{}", reply);
		}
		log.info("fileVO:{}", fileVO);

		model.addAttribute("vo", outVO);
		model.addAttribute("replyList", replyList);
		model.addAttribute("fileName", fileVO);

		return "board/board_mng";
	}

	@GetMapping(value = "/doDelete.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doDelete(@RequestParam(name = "boardId", required = true, defaultValue = "0") int boardId)
			throws SQLException {
		String jsonString = "";

		log.info("┌──────────────────────────┐");
		log.info("│ doDelete()               │");
		log.info("└──────────────────────────┘");

		BoardVO boardVO = new BoardVO();
		boardVO.setBoardId(boardId);
		log.info("boardId:{}", boardId);

		int flag = this.boardService.doDelete(boardVO);
		log.info("flag:{}", flag);

		String message = "";
		if (1 == flag) {
			message = "게시글이 삭제 되었습니다.";
		} else {
			message = "게시글 삭제 실패.";
		}

		jsonString = new Gson().toJson(new MessageVO(flag, message));
		log.info("jsonString:\n{}", jsonString);

		return jsonString;
	}

	@PostMapping(value = "/doUpdate.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doUpdate(BoardVO boardVO, @RequestParam(value = "file", required = false) MultipartFile file,
			HttpSession session) throws SQLException {
		String jsonString = "";
		log.info("┌──────────────────────────┐");
		log.info("│ doUpdate()               │");
		log.info("└──────────────────────────┘");

		log.info("boardVO: {}", boardVO);

		// 게시글 수정 처리
		int flag = boardService.doUpdate(boardVO);
		String message = "";
		if (1 == flag) {
			message = "게시글이 수정되었습니다.";

			// 파일 업로드 처리
			if (file != null && !file.isEmpty()) {
				FileVO vo = new FileVO();
				String orgFileName = file.getOriginalFilename();
				long fileSize = file.getSize();
				String saveFileName = StringUtil.getPK("yyyyMMddHHmmss");
				String ext = StringUtil.getExt(orgFileName);

				try {
					// 파일 저장 정보 설정
					vo.setOrgFileName(orgFileName);
					vo.setFileSize(fileSize);
					vo.setSavePath(fileFullPath); // 파일 저장 경로
					vo.setSaveFileName(saveFileName + "." + ext);
					vo.setExtension(ext);
					vo.setBoardId(boardVO.getBoardId()); // 게시글 ID 설정

					log.info("orgFileName: {}", orgFileName);
					log.info("fileSize: {}", fileSize);
					log.info("fileFullPath: {}", fileFullPath);
					log.info("saveFileName: {}", saveFileName + "." + ext);
					log.info("ext: {}", ext);
					log.info("boardId: {}", vo.getBoardId());

					// 파일을 지정한 경로에 저장
					file.transferTo(new File(fileFullPath + File.separator + vo.getSaveFileName()));

					// 파일 저장 후, DB에 파일 정보를 추가 저장
					this.fileService.saveFile(vo); // 파일 정보를 DB에 저장하는 서비스 호출

					// message += " 파일이 업로드 되었습니다."; // Optionally, append this message
				} catch (IllegalStateException | IOException e) {
					message = "게시글 수정 실패: 파일 업로드 중 오류 발생 - " + e.getMessage();
				}
			}

		} else {
			message = "게시글 수정 실패.";
		}

		// 결과 메시지와 플래그를 담아서 클라이언트로 반환
		MessageVO messageVO = new MessageVO(flag, message);
		jsonString = new Gson().toJson(messageVO);
		log.info("jsonString:\n{}", jsonString);

		return jsonString;
	}

	@PostMapping(value = "/doUpdate1.do", produces = "text/plain;charset=UTF-8")
	@ResponseBody
	public String doUpdate1(BoardVO boardVO, HttpSession session) throws SQLException {
		String jsonString = "";
		log.info("┌──────────────────────────┐");
		log.info("│ doUpdate()               │");
		log.info("└──────────────────────────┘");

//		if(null != session.getAttribute("user")) {
//			UserVO user =(UserVO) session.getAttribute("user");
//			param.setModId(user.getUserId());
//			
//		}

		log.info("boardVO:{}", boardVO);

		int flag = boardService.doUpdate(boardVO);
		String message = "";
		if (1 == flag) {
			message = "게시글이 수정 되었습니다.";
		} else {
			message = "게시글이 등록 되었습니다.";
		}

		MessageVO messageVO = new MessageVO(flag, message);

		jsonString = new Gson().toJson(messageVO);
		log.info("jsonString:\n{}", jsonString);

		return jsonString;
	}

}
