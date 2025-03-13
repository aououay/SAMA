document.addEventListener('DOMContentLoaded',function(){
    console.log('DOMContentLoaded');

    const doRetrieveButton =document.querySelector("#doRetrieveBtn");
    console.log(doRetrieveButton);

    const pageNoInput     = document.querySelector("#pageNo");
    const searchDivSelect = document.querySelector("#searchDiv");
    const searchWordInput = document.querySelector("#searchWord");
    const pageSizeSelect  = document.querySelector("#pageSize");

    //모든 rows (데이터들)
    const rows = document.querySelectorAll("#listTable>tbody>tr");

    const userForm        = document.querySelector("#userForm");
    doRetrieveButton.addEventListener('click',function(event){
        event.preventDefault(); //버블링 방지

        userForm.pageNo.value = 1;
        userForm.action = "/disastermessage/view";

        userForm.submit();

    });



});

function pageDoRetrieve (url, pageNo) {
    let userForm = document.userForm;
    userForm.pageNo.value = pageNo;
    userForm.action = url;
    userForm.submit();
}

document.addEventListener('DOMContentLoaded', function () {
    // 검색 초기화 버튼 클릭 이벤트 리스너 추가
    document.getElementById('reset').addEventListener('click', function () {
        // 검색 필드 초기화
        document.getElementById('searchWord').value = '';

        // 검색 조건 초기화
        const searchDiv = document.getElementById('searchDiv');
        searchDiv.value = '10';  // 기본값 (전체 선택)

        // 페이지 크기 초기화 (기본값 10)
        const pageSize = document.getElementById('pageSize');
        pageSize.value = '10';  // 기본값 10

        // 페이지 번호 초기화 (hidden 필드에 설정)
        document.getElementById('pageNo').value = '1'; // 기본적으로 1 페이지로 초기화

        // 폼을 다시 제출할 수 있도록 페이지번호를 다시 설정
        document.getElementById('userForm').submit();
    });
});