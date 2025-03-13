// URL 파라미터에서 특정 값을 가져오는 함수
function getQueryParameter(name) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(name);
}

// 페이지 로드 시 searchDiv와 searchWord에 맞는 상태를 반영
window.onload = function() {
    const searchDiv = getQueryParameter('searchDiv');
    const searchWord = getQueryParameter('searchWord');

    // searchDiv에 맞는 항목에 'on' 클래스를 추가
    if (searchDiv === '10') {
        document.getElementById('listDate').classList.add('on');
    } else if (searchDiv === '20') {
        document.getElementById('listMng').classList.add('on');
    }

    // 'year' select 태그에서 searchWord에 해당하는 연도 선택
    if (searchWord) {
        document.getElementById('year').value = searchWord;
    }
};

// 연도 선택 시 해당 연도에 맞는 데이터 조회
function changeYear() {
    const year = document.getElementById('year').value;
    const currentSearchDiv = getQueryParameter('searchDiv'); // 기존에 선택된 searchDiv 값 가져오기
    let newUrl = '/earthquake/view?searchWord=' + year;

    // searchDiv 값이 있을 경우, 그대로 유지하면서 새 URL 생성
    if (currentSearchDiv) {
        newUrl += '&searchDiv=' + currentSearchDiv;
    }

    // 새로운 URL로 리디렉션
    window.location.href = newUrl;
}

// 클릭 시 선택된 항목 변경
function changeSelection(value) {
    const currentSearchWord = getQueryParameter('searchWord'); // 기존에 선택된 searchWord 값 가져오기
    let newUrl = '/earthquake/view?searchDiv=' + value;

    // searchWord 값이 있을 경우, 그대로 유지하면서 새 URL 생성
    if (currentSearchWord) {
        newUrl += '&searchWord=' + currentSearchWord;
    }

    // 새로운 URL로 리디렉션
    window.location.href = newUrl;
}


