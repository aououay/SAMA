<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>재난 상황 대처 퀴즈</title>
  <!-- Bootstrap 5 CDN -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <!-- Google Fonts (Roboto) -->
  <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
  <link href="/assets/css/quiz.css" rel="stylesheet">

</head>
<body>

  <div class="container">
    <div class="card quiz-card">
      <div class="card-header text-center">
        <h2>재난 상황 대처 퀴즈</h2>
      </div>
      <div class="card-body">
        <form action="${pageContext.request.contextPath}/quiz/quizResult" method="post">
          <!-- 이름 입력 -->
          <div class="mb-4 text-center">
            <input type="text" id="userName" name="userName" class="form-control w-50 mx-auto" placeholder="이름을 입력해주세요" required>
          </div>

      <!-- 문항 1 -->
      <div class="question active" id="question-0">
		  <h4>1. 재난이 발생했을 때, 내가 먼저 보이는 반응은?</h4>
		  <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
		    <input type="radio" class="btn-check" name="answers" id="q0_opt1" value="1" autocomplete="off" required>
		    <label class="btn btn-outline-primary" for="q0_opt1">전혀 반응하지 않는다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q0_opt2" value="2" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q0_opt2">차분하게 상황을 살핀다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q0_opt3" value="3" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q0_opt3">즉시 반응하지만 혼란스럽다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q0_opt4" value="4" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q0_opt4">빠르게 반응하며 주변을 돕는다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q0_opt5" value="5" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q0_opt5">매우 빠르게 반응하고 상황을 주도한다</label>
		  </div>
		</div>


      <!-- 문항 2 -->
      <div class="question" id="question-1">
		  <h4>2. 재난 상황에서 내가 가장 우선적으로 하는 일은 무엇인가요?</h4>
		  <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
		    <input type="radio" class="btn-check" name="answers" id="q1_opt1" value="1" autocomplete="off" required>
		    <label class="btn btn-outline-primary" for="q1_opt1">신경쓰지 않고 하던일을 한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q1_opt2" value="2" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q1_opt2">다른 사람에게 도움을 청한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q1_opt3" value="3" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q1_opt3">주변 상황을 신속히 파악한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q1_opt4" value="4" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q1_opt4">상황을 분석하고 대응 계획을 세운다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q1_opt5" value="5" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q1_opt5">즉시 행동에 나선다</label>
		  </div>
		</div>


      <!-- 문항 3 -->
      <div class="question" id="question-2">
		  <h4>3. 위험을 느낄 때 내가 취하는 행동은?</h4>
		  <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
		    <input type="radio" class="btn-check" name="answers" id="q2_opt1" value="1" autocomplete="off" required>
		    <label class="btn btn-outline-primary" for="q2_opt1">위험을 전혀 느끼지 못한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q2_opt2" value="2" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q2_opt2">위험을 느끼지만 무시한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q2_opt3" value="3" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q2_opt3">위험을 느끼고 경계한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q2_opt4" value="4" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q2_opt4">위험에 즉시 대응한다</label>
		
		    <input type="radio" class="btn-check" name="answers" id="q2_opt5" value="5" autocomplete="off">
		    <label class="btn btn-outline-primary" for="q2_opt5">위험을 분석하고 즉각적인 조치를 취한다</label>
		  </div>
		</div>


      <!-- 문항 4 -->
      <div class="question" id="question-3">
        <h4>4. 재난 상황에서 다른 사람들과 협력하는 것에 대해 나는 어떻게 느끼는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q3_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q3_opt1">협력이 불필요하다</label>

          <input type="radio" class="btn-check" name="answers" id="q3_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q3_opt2">협력이 불편하다</label>

          <input type="radio" class="btn-check" name="answers" id="q3_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q3_opt3">협력이 중요하지만 혼자서도 가능하다</label>

          <input type="radio" class="btn-check" name="answers" id="q3_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q3_opt4">협력이 중요하다</label>

          <input type="radio" class="btn-check" name="answers" id="q3_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q3_opt5">협력이 매우 중요하다</label>
        </div>
      </div>

      <!-- 문항 5 -->
      <div class="question" id="question-4">
        <h4>5. 불확실한 상황에서 나는 어떻게 대처하는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q4_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q4_opt1">아무것도 하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q4_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q4_opt2">상황을 회피한다</label>

          <input type="radio" class="btn-check" name="answers" id="q4_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q4_opt3">상황을 관망한다</label>

          <input type="radio" class="btn-check" name="answers" id="q4_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q4_opt4">상황을 분석 후 결정한다</label>

          <input type="radio" class="btn-check" name="answers" id="q4_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q4_opt5">빠르게 결정하고 행동한다</label>
        </div>
      </div>

      <!-- 문항 6 -->
      <div class="question" id="question-5">
        <h4>6. 재난 후, 나의 가장 중요한 우선순위는 무엇인가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q5_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q5_opt1">자포자기 상태가 된다</label>

          <input type="radio" class="btn-check" name="answers" id="q5_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q5_opt2">내 안전이 최우선이다</label>

          <input type="radio" class="btn-check" name="answers" id="q5_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q5_opt3">생존을 최우선으로 하되 주변도 신경쓴다</label>

          <input type="radio" class="btn-check" name="answers" id="q5_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q5_opt4">안전을 최우선으로, 주변을 적극 지원한다</label>

          <input type="radio" class="btn-check" name="answers" id="q5_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q5_opt5">상황 해결을 위해 협력한다</label>
        </div>
      </div>

      <!-- 문항 7 -->
      <div class="question" id="question-6">
        <h4>7. 재난 상황에서 스트레스를 어떻게 관리하는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q6_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q6_opt1">스트레스가 극복되지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q6_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q6_opt2">스트레스를 무시한다</label>

          <input type="radio" class="btn-check" name="answers" id="q6_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q6_opt3">스트레스를 관리하려 애쓴다</label>

          <input type="radio" class="btn-check" name="answers" id="q6_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q6_opt4">스트레스를 효과적으로 다룬다</label>

          <input type="radio" class="btn-check" name="answers" id="q6_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q6_opt5">스트레스를 잘 관리한다</label>
        </div>
      </div>

      <!-- 문항 8 -->
      <div class="question" id="question-7">
        <h4>8. 위험을 피하기 위한 내 행동은?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q7_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q7_opt1">전혀 행동하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q7_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q7_opt2">상황을 피해 숨는다</label>

          <input type="radio" class="btn-check" name="answers" id="q7_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q7_opt3">위험 회피 방법을 모색한다</label>

          <input type="radio" class="btn-check" name="answers" id="q7_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q7_opt4">빠르게 행동해 위험을 피한다</label>

          <input type="radio" class="btn-check" name="answers" id="q7_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q7_opt5">즉시 안전한 장소로 이동한다</label>
        </div>
      </div>

      <!-- 문항 9 -->
      <div class="question" id="question-8">
        <h4>9. 재난 상황에서 나는 다른 사람들에게 얼마나 조언을 주는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q8_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q8_opt1">전혀 조언하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q8_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q8_opt2">간헐적으로 조언한다</label>

          <input type="radio" class="btn-check" name="answers" id="q8_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q8_opt3">상황에 따라 조언한다</label>

          <input type="radio" class="btn-check" name="answers" id="q8_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q8_opt4">자주 조언한다</label>

          <input type="radio" class="btn-check" name="answers" id="q8_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q8_opt5">항상 조언하며 이끈다</label>
        </div>
      </div>

      <!-- 문항 10 -->
      <div class="question" id="question-9">
        <h4>10. 재난 상황에서 내가 상황을 통제하는 방식은?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q9_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q9_opt1">통제하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q9_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q9_opt2">통제하려 하지만 어렵다</label>

          <input type="radio" class="btn-check" name="answers" id="q9_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q9_opt3">통제하려고 노력한다</label>

          <input type="radio" class="btn-check" name="answers" id="q9_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q9_opt4">빠르게 통제한다</label>

          <input type="radio" class="btn-check" name="answers" id="q9_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q9_opt5">완벽하게 통제한다</label>
        </div>
      </div>

      <!-- 문항 11 -->
      <div class="question" id="question-10">
        <h4>11. 재난이 끝난 후, 내 첫 번째 행동은?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q10_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q10_opt1">아무것도 하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q10_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q10_opt2">혼자서 정리한다</label>

          <input type="radio" class="btn-check" name="answers" id="q10_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q10_opt3">주변인들과 함께 사태파악을 한다</label>

          <input type="radio" class="btn-check" name="answers" id="q10_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q10_opt4">복구 작업에 참여한다</label>

          <input type="radio" class="btn-check" name="answers" id="q10_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q10_opt5">즉시 다른 사람을 돕는다</label>
        </div>
      </div>

      <!-- 문항 12 -->
      <div class="question" id="question-11">
        <h4>12. 재난 상황에서 내가 가장 중요한 자원으로 여기는 것은?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q11_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q11_opt1">혼자 살아남는 능력</label>

          <input type="radio" class="btn-check" name="answers" id="q11_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q11_opt2">타인의 도움</label>

          <input type="radio" class="btn-check" name="answers" id="q11_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q11_opt3">물자와 도구</label>

          <input type="radio" class="btn-check" name="answers" id="q11_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q11_opt4">지식과 경험</label>

          <input type="radio" class="btn-check" name="answers" id="q11_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q11_opt5">사람들과의 협력</label>
        </div>
      </div>

      <!-- 문항 13 -->
      <div class="question" id="question-12">
        <h4>13. 재난 상황에서 나는 얼마나 빠르게 판단을 내리는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q12_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q12_opt1">전혀 판단하지 못한다</label>

          <input type="radio" class="btn-check" name="answers" id="q12_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q12_opt2">판단이 어렵다</label>

          <input type="radio" class="btn-check" name="answers" id="q12_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q12_opt3">상황을 보고 판단한다</label>

          <input type="radio" class="btn-check" name="answers" id="q12_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q12_opt4">빠르게 판단한다</label>

          <input type="radio" class="btn-check" name="answers" id="q12_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q12_opt5">즉시 판단하고 움직인다</label>
        </div>
      </div>

      <!-- 문항 14 -->
      <div class="question" id="question-13">
        <h4>14. 재난 상황에서 나의 대처 방법은 어떤가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q13_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q13_opt1">전혀 대처하지 못한다</label>

          <input type="radio" class="btn-check" name="answers" id="q13_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q13_opt2">상황을 피해 도망친다</label>

          <input type="radio" class="btn-check" name="answers" id="q13_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q13_opt3">대처 방법을 모색한다</label>

          <input type="radio" class="btn-check" name="answers" id="q13_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q13_opt4">신속히 대처 결정을 내린다</label>

          <input type="radio" class="btn-check" name="answers" id="q13_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q13_opt5">즉시 대처하고 실행한다</label>
        </div>
      </div>

      <!-- 문항 15 -->
      <div class="question" id="question-14">
        <h4>15. 재난 상황에서 나는 다른 사람들과 얼마나 소통하는가?</h4>
        <div class="btn-group btn-group-toggle d-flex flex-wrap" data-toggle="buttons">
          <input type="radio" class="btn-check" name="answers" id="q14_opt1" value="1" autocomplete="off" required>
          <label class="btn btn-outline-primary" for="q14_opt1">전혀 소통하지 않는다</label>

          <input type="radio" class="btn-check" name="answers" id="q14_opt2" value="2" autocomplete="off">
          <label class="btn btn-outline-primary" for="q14_opt2">간헐적으로 소통한다</label>

          <input type="radio" class="btn-check" name="answers" id="q14_opt3" value="3" autocomplete="off">
          <label class="btn btn-outline-primary" for="q14_opt3">상황에 따라 소통한다</label>

          <input type="radio" class="btn-check" name="answers" id="q14_opt4" value="4" autocomplete="off">
          <label class="btn btn-outline-primary" for="q14_opt4">자주 소통한다</label>

          <input type="radio" class="btn-check" name="answers" id="q14_opt5" value="5" autocomplete="off">
          <label class="btn btn-outline-primary" for="q14_opt5">항상 소통하고 조율한다</label>
        </div>
      </div>

	  <!-- 네비게이션 버튼 영역 -->
       <div id="navigation" class="navigation-buttons">
         <button type="button" id="prevBtn" class="btn btn-outline-primary">이전</button>
         <button type="button" id="nextBtn" class="btn btn-primary">다음</button>
         <button type="submit" id="submitBtn" class="btn btn-success">결과 보기</button>
       </div>	  	  
    </form>
  </div>

  <!-- Bootstrap JS (Popper 포함) -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  <!-- 문항 전환용 JavaScript -->
  <script src="/assets/js/quiz.js"></script>
</body>
</html>
