// URL 파라미터에서 특정 값을 가져오는 함수
function getQueryParameter(name) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(name);
}

// 페이지 로드 시 searchDiv와 searchWord에 맞는 상태를 반영
window.onload = function() {
    const searchDiv = getQueryParameter('searchDiv');
    const searchWord = getQueryParameter('searchWord');



    // 'year' select 태그에서 searchWord에 해당하는 연도 선택
    if (searchWord) {
        document.getElementById('year').value = searchWord;
    }
};

// 연도 선택 시 해당 연도에 맞는 데이터 조회
function changeYear() {
    const year = document.getElementById('year').value; // 선택된 연도 값
    let urlParams = new URLSearchParams(window.location.search);

    // 선택된 연도를 searchWord로 설정
    urlParams.set('searchWord', year);

    // searchDiv 값을 초기화 (없애기)
    urlParams.delete('searchDiv'); // 기존의 searchDiv 값을 제거

    // 새로운 URL로 리디렉션 (페이지 새로고침)
    window.location.search = urlParams.toString();
}

// 클릭 시 선택된 항목 변경
function changeSelection(value) {
    const currentSearchWord = getQueryParameter('searchWord'); // 기존에 선택된 searchWord 값 가져오기

    let urlParams = new URLSearchParams(window.location.search);
    urlParams.set('searchDiv', value); // searchDiv 값을 새로 설정

    // searchWord 값이 있을 경우, 그대로 유지하면서 새 URL 생성
    if (currentSearchWord) {
        urlParams.set('searchWord', currentSearchWord); // 기존의 searchWord 값 유지
    }

    // 새로운 URL로 리디렉션 (페이지 새로고침)
    window.location.search = urlParams.toString();
}
