package com.pcwk.ehr.quiz.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@RequestMapping("/quiz")  // 기본 URL 패턴 설정
@Controller
public class QuizController {

    // 퀴즈 페이지 표시
    @GetMapping("/quiz")
    public String showQuiz() {
        return "quiz/quiz";  // /WEB-INF/views/quiz/quiz.jsp로 포워딩
    }

    @PostMapping("/quizResult")
    public String processQuizResult(@RequestParam("userName") String userName,
                                    @RequestParam(value = "answers", required = false) List<Integer> answers,
                                    Model model) {
        // null 체크: 사용자가 답변을 선택하지 않은 경우 빈 배열로 초기화
        if (answers == null || answers.isEmpty()) {
            answers = new ArrayList<>();
        }

        // 성향 점수 배열 초기화 (A부터 O까지)
        int[] scores = new int[15];  // A부터 O까지 총 15개 성향
        String[] types = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"};

        // 각 질문에 대해 점수 부여
        for (int answer : answers) {
            if (answer >= 1 && answer <= 15) {  // 1부터 15까지의 답변을 성향에 매칭
                scores[answer - 1]++;  // 배열 인덱스를 맞추기 위해 -1
            }
        }

        // 가장 높은 점수를 가진 성향 찾기
        int maxScore = scores[0];
        String dominantType = types[0];  // 첫 번째 성향으로 시작

        for (int i = 1; i < scores.length; i++) {
            if (scores[i] > maxScore) {
                maxScore = scores[i];
                dominantType = types[i];
            }
        }

        // 성향 분석 및 MBTI 유형 결정
        String result = "MBTI 유형: ";
        String description = "";
        String imageUrl = "";  // 이미지 URL 변수 추가

        switch (dominantType) {
            case "A":
                result += "ISTJ";
                description = "ISTJ는 매우 신중하고 분석적인 성향을 가진 사람입니다. 위급한 상황에서 차분하게 상황을 분석하고 대처하는 편입니다. "
                        + "책임감이 강하고, 절차와 규칙을 중시하며, 세부 사항에 강한 집중력을 발휘합니다. "
                        + "상황을 정확하게 평가하고 신뢰할 수 있는 방법을 찾아 안정감을 추구합니다. "
                        + "과거의 경험에서 배우며, 예측 가능한 미래를 중시합니다. "
                        + "직장 내에서는 규율과 체계적인 일을 선호하며, 안정적인 환경을 좋아합니다. "
                        + "가족이나 친구들에게도 신뢰를 주는 편이고, 도움을 필요로 하는 사람들에게는 끊임없이 지원을 제공합니다. "
                        + "위급 상황에서는 침착하게 상황을 분석하고 최선의 결정을 내리는 타입입니다.";
                imageUrl = "/assets/images/in_kyo/istj.png";
                break;
            case "B":
                result += "ISFJ";
                description = "ISFJ는 협력적이고 사람들과의 관계를 중요하게 생각하는 성향입니다. 재난 상황에서도 다른 사람들과 함께 해결책을 찾고, 이들을 돕기 위해 노력합니다. "
                        + "타인의 감정을 세심하게 살피며, 공감 능력이 뛰어나 사람들에게 따뜻한 감정을 전달합니다. "
                        + "고요하고 차분한 성격으로, 평화롭고 조화로운 환경에서 편안함을 느낍니다. "
                        + "자신의 신뢰를 쌓은 사람들에게 헌신적인 모습을 보이며, 불편한 상황에서는 갈등을 최소화하려 노력합니다. "
                        + "타인의 필요를 먼저 생각하고, 자신보다 다른 사람을 배려하는 경향이 강합니다. "
                        + "재난 상황에서 빠르게 위험을 감지하고, 타인의 안전을 먼저 고려하며 행동합니다. "
                        + "이러한 성향은 공동체나 집단 내에서 중요한 역할을 합니다.";
                imageUrl = "/assets/images/in_kyo/isfj.png";
                break;
            case "C":
                result += "INFP";
                description = "INFP는 창의적이고 유연한 성향을 가진 사람입니다. 위급한 상황에서도 새로운 해결책을 찾으려고 하며, 남들과 다른 방식으로 대처할 수 있는 능력을 가집니다. "
                        + "내면의 가치와 신념을 중요하게 생각하며, 이상적인 세상을 꿈꾸고 이를 현실로 만들기 위해 노력합니다. "
                        + "감성적이고 직관적인 성격으로, 다른 사람들의 감정을 잘 이해하고 공감하는 능력이 뛰어납니다. "
                        + "자유롭고 창의적인 활동을 좋아하며, 사회적 압박을 받기보다 자신만의 페이스로 살아가기를 원합니다. "
                        + "재난 상황에서 비상 상황을 창의적인 방법으로 해결하려 하며, 기존의 틀을 벗어나 새로운 해결책을 제시할 수 있습니다. "
                        + "혼자만의 시간을 소중하게 여기고, 자신의 내면을 탐구하며 감정을 표현하는데 어려움을 느끼기도 합니다. "
                        + "인간 관계에서 깊은 신뢰를 중요시하며, 한 사람에게 매우 충실한 친구입니다.";
                imageUrl = "/assets/images/in_kyo/infp.png";
                break;
            case "D":
                result += "ESTJ";
                description = "ESTJ는 적극적이고 현실적인 성향을 가진 사람입니다. 위급한 상황에서 빠르게 결정을 내리고 즉각적으로 행동하는 경향이 강합니다. "
                        + "논리적이고 체계적인 사고 방식을 중시하며, 문제를 해결하기 위한 실용적인 방법을 찾습니다. "
                        + "효율성과 결과를 중시하며, 목표 달성을 위해 필요 없는 것을 제거하고 필수적인 일에 집중합니다. "
                        + "주어진 규칙을 따르며, 법과 규율을 중요시합니다. "
                        + "가족, 친구, 직장 내에서 자신감을 가지고 리더십을 발휘하며, 조직적인 환경을 선호합니다. "
                        + "재난 상황에서도 빠르게 상황을 분석하고, 그에 맞는 해결책을 즉시 제시할 수 있는 능력이 뛰어납니다. "
                        + "현실적이고 목표 지향적인 성향으로, 위기 상황에서 실용적인 접근을 통해 문제를 해결하는 데 강점을 보입니다.";
                imageUrl = "/assets/images/in_kyo/estj.png";
                break;
            case "E":
                result += "ESTP";
                description = "ESTP는 위급 상황에서 매우 침착하고 빠르게 행동하는 성향입니다. 재난 상황에서 다른 사람들과 협력하고 상황을 빠르게 처리하려는 성향이 강합니다. "
                        + "실용적이고 현실적인 성격을 가진 당신은 즉각적인 해결책을 찾고, 당면한 문제를 해결하는 데 있어 효율적입니다. "
                        + "모험을 즐기고, 새로운 경험을 통해 성장하는 것을 중요하게 생각합니다. "
                        + "사람들과의 관계에서 직접적이고 솔직한 스타일을 선호하며, 자신의 의견을 자유롭게 표현합니다. "
                        + "위급 상황에서는 신속하게 상황을 분석하고, 즉각적인 결정을 내립니다. "
                        + "때로는 대담하고 도전적인 행동을 통해 상황을 해결하는 경향이 있습니다. "
                        + "사람들의 신뢰를 얻기 위해 기민하고 적극적인 태도를 보이며, 현실적이고 결과 지향적인 접근을 좋아합니다.";
                imageUrl = "/assets/images/in_kyo/estp.png";
                break;

            case "F":
                result += "INTJ";
                description = "INTJ는 독립적이고 분석적인 성향을 가진 사람입니다. 높은 지적 능력과 독창성을 지니며, 복잡한 문제를 해결하는 데 능숙합니다. "
                        + "모든 일을 계획하고 전략적으로 접근하며, 미래의 목표를 달성하기 위해 끊임없이 준비합니다. "
                        + "자신의 목표를 이루기 위해 대담하고 혁신적인 아이디어를 추진하며, 타인의 의견에 큰 영향을 받지 않습니다. "
                        + "기존의 방법에 얽매이지 않고 새로운 해결책을 모색합니다. "
                        + "위급한 상황에서 전략적인 분석을 통해 해결책을 빠르게 도출해내는 성향을 지니고 있습니다. "
                        + "자신의 내적인 세계에서 많은 시간을 보내며, 논리적이고 합리적인 사고를 중시합니다.";
                imageUrl = "/assets/images/in_kyo/intj.png";
                break;
            case "G":
                result += "INTP";
                description = "INTP는 창의적이고 분석적인 성향을 가진 사람입니다. 복잡한 개념을 탐구하고, 창의적인 아이디어를 내는 데 탁월합니다. "
                        + "논리적이고 독립적인 사고를 통해 문제를 해결하며, 새로운 이론이나 시스템을 구상하는 능력이 뛰어납니다. "
                        + "사회적인 규칙이나 기존의 체제에 구애받지 않으며, 개인의 자유를 중시합니다. "
                        + "위급 상황에서 창의적인 해결책을 제시하며, 새로운 방식으로 문제를 해결하려 합니다. "
                        + "이성적이고 분석적인 접근을 선호하며, 감정보다는 논리에 기반한 결정을 내립니다.";
                imageUrl = "/assets/images/in_kyo/intp.png";
                break;
            case "H":
                result += "INFJ";
                description = "INFJ는 깊은 내면의 세계를 중시하는 이상주의자입니다. 타인의 필요에 민감하고, 직관적이고 창의적인 성향을 지닙니다. "
                        + "사람들을 돕기 위해 항상 노력하며, 이상적인 세상에 대한 비전을 가지고 있습니다. "
                        + "타인의 감정을 깊이 이해하고, 이를 바탕으로 상호작용하며, 신뢰를 중요하게 생각합니다. "
                        + "복잡한 감정을 잘 표현하지 않지만, 그 내면에서 강한 사명감을 가지고 사람들을 돕기 위해 헌신합니다. "
                        + "재난 상황에서도 타인의 안전을 우선시하며, 비전을 통해 문제 해결을 제시할 수 있는 능력이 있습니다.";
                imageUrl = "/assets/images/in_kyo/infj.png";
                break;
            case "I":
                result += "INFP";
                description = "INFP는 이상주의적이고 창의적인 성향을 가진 사람입니다. 자아를 깊이 탐구하며, 높은 도덕적 기준을 가지고 있습니다. "
                        + "자신의 신념과 가치를 매우 중요시하며, 타인에게 감정적으로 연결된 관계를 형성합니다. "
                        + "자유롭고 열린 마음을 가지고 있으며, 정해진 규칙보다는 자신의 내적인 직관에 따라 삶을 이끌어갑니다. "
                        + "위급 상황에서는 기존의 틀을 벗어난 해결책을 제시할 수 있으며, 창의적이고 직관적인 방법으로 문제를 해결하려 합니다. "
                        + "타인의 행복을 위해 희생하는 것을 감수할 수 있는 이상적인 성격입니다.";
                imageUrl = "/assets/images/in_kyo/infp.png";
                break;
            case "J":
                result += "ENFP";
                description = "ENFP는 열정적이고 창의적인 성격을 가진 사람입니다. 아이디어와 가능성에 대해 항상 열린 마음을 가지고 있습니다. "
                        + "타인과의 상호작용에서 활발하며, 새로운 경험을 추구합니다. 상상력과 직관을 통해 문제를 해결하며, 사람들에게 긍정적인 영향을 미칩니다. "
                        + "위급 상황에서도 적극적이고 빠르게 문제를 해결하려는 성향을 보입니다. "
                        + "어떤 일이든 창의적이고 유연하게 접근하며, 항상 새로운 해결책을 모색하려는 태도를 보입니다. "
                        + "타인에게 영감을 주고, 그룹 내에서 활발하게 아이디어를 공유하며, 분위기를 이끌어가는 역할을 합니다.";
                imageUrl = "/assets/images/in_kyo/enfp.png";
                break;
            case "K":
                result += "ENFJ";
                description = "ENFJ는 강력한 리더십과 대인 관계에서의 강한 직관을 가진 사람입니다. 사람들의 잠재력을 발견하고, 그들이 성장할 수 있도록 돕는 데 능숙합니다. "
                        + "타인을 돕는 것을 중요시하며, 이타적이고, 사람들의 감정을 이해하는 데 뛰어난 능력을 보입니다. "
                        + "자신의 의견을 설득력 있게 전달하고, 사람들을 함께 이끌어가며 팀워크를 중요시합니다. "
                        + "위급 상황에서는 신속하고 효율적으로 그룹을 조직하여 문제를 해결하려 하며, 사람들의 안전을 최우선으로 생각합니다. "
                        + "높은 사회적 인식을 가지고 있으며, 사람들 사이의 갈등을 해결하는 능력이 뛰어납니다.";
                imageUrl = "/assets/images/in_kyo/enfj.png";
                break;
            case "L":
                result += "ESFJ";
                description = "ESFJ는 친절하고 지원적인 성향을 가진 사람입니다. 사람들과의 관계를 중요시하며, 타인의 필요에 민감하게 반응합니다. "
                        + "주변 사람들에게 도움을 주고자 하며, 공동체의 조화와 안정을 추구합니다. "
                        + "가족이나 친구들과의 관계에서 신뢰를 쌓고, 항상 다른 사람들을 위해 최선을 다합니다. "
                        + "위급 상황에서도 다른 사람들을 돕기 위해 뛰어난 협력적인 태도를 보이며, 주변 사람들에게 위로와 지원을 제공합니다. "
                        + "일상에서 긍정적인 에너지를 전파하며, 사람들과의 관계에서 따뜻함을 제공합니다.";
                imageUrl = "/assets/images/in_kyo/esfj.png";
                break;
            case "M":
                result += "ISTP";
                description = "ISTP는 분석적이고 실용적인 성향을 가진 사람입니다. 문제를 빠르게 해결하는 능력이 뛰어나며, 기술적인 문제를 해결하는 데 뛰어난 재능을 가지고 있습니다. "
                        + "직접 경험을 통해 배우는 성향이 강하고, 비판적 사고를 통해 문제를 분석하고 해결합니다. "
                        + "위급 상황에서는 빠르게 상황을 파악하고, 실용적인 해결책을 즉시 도출할 수 있는 능력이 뛰어납니다. "
                        + "자유롭고 유연한 환경을 선호하며, 규칙에 얽매이지 않고 창의적인 방법으로 일을 처리합니다. "
                        + "때로는 다른 사람들과의 관계에서 거리감을 두기도 하지만, 중요한 순간에는 실용적인 지원을 제공합니다.";
                imageUrl = "/assets/images/in_kyo/istp.png";
                break;
            case "N":
                result += "ISFP";
                description = "ISFP는 조용하고 예술적인 성향을 가진 사람입니다. 감성적이고, 자연과의 연결을 중요시하며, 자신만의 방식으로 세상을 바라봅니다. "
                        + "자유로운 영혼을 가지고 있으며, 규칙보다는 직관을 따르는 경향이 있습니다. "
                        + "위급 상황에서 창의적이고 감성적인 해결책을 제시하려 하며, 다른 사람들의 감정을 존중하는 성향이 강합니다. "
                        + "조용하게 자신의 시간을 보내며, 내면의 세계를 탐구하는 것을 좋아합니다. "
                        + "타인에게 긍정적인 에너지를 전파하며, 깊은 감정을 잘 표현하지 않지만, 중요한 순간에 중요한 지원을 제공합니다.";
                imageUrl = "/assets/images/in_kyo/isfp.png";
                break;
            case "O":
                result += "ESFP";
                description = "ESFP는 사교적이고 활동적인 성향을 가진 사람입니다. 사람들과 함께 시간을 보내는 것을 좋아하며, 삶을 즐기고 에너지를 발산하는 성격입니다. "
                        + "위급 상황에서는 즉각적으로 반응하고, 빠르게 행동하여 상황을 개선하려는 성향을 가집니다. "
                        + "사람들의 감정을 잘 읽고, 분위기를 잘 파악하여 긍정적인 영향을 미칩니다. "
                        + "새로운 경험을 추구하고, 자유로운 환경을 선호하며, 타인의 기분을 즐겁게 만드는 데 능숙합니다. "
                        + "자신의 직감을 따르며, 예측할 수 없는 상황에서도 신속하게 대응할 수 있습니다.";
                imageUrl = "/assets/images/in_kyo/esfp.png";
                
                break;

        }

        // 모델에 결과 추가
        model.addAttribute("result", result);
        model.addAttribute("description", description);
        model.addAttribute("userName", userName);
        model.addAttribute("imageUrl", imageUrl); 

        return "quiz/quizResult"; // /WEB-INF/views/quiz/quizResult.jsp로 포워딩
    }
}
