document.addEventListener('DOMContentLoaded',function(){
  console.log('DOMContentLoaded');
  const div = document.getElementById('div');
  console.log(div)
  
  
  const moveToRegButton = document.querySelector("#moveToRegBtn");
  moveToRegButton.addEventListener("click",function(event){
    console.log("moveToRegButton click");
    moveToReg();
  });

  function moveToReg() {
    console.log("moveToReg()");
    //if (confirm('등록 화면으로 이동 하시겠습니까?') == false)return;
		
    window.location.href = '/board/board_reg_index.do?div='+div.value;
}	
  

  const doRetrieveButton = document.querySelector("#doRetrieveBtn");

  doRetrieveButton.addEventListener("click",function(event){
    console.log("doRetrieveButton click");
    event.preventDefault();

    doRetrieve(1); 
  });


  function doRetrieve(pageNo){
    console.log("doRetrieve pageNo :"+pageNo);

    const userForm = document.userForm;
    userForm.pageNo.value = pageNo;
    userForm.action = "/board/doRetrieve.do";

    userForm.submit();
  }

}); 


function pageDoRetrieve(url,pageNo){
  console.log("pageDoRetrieve click");                

  let userForm = document.userForm; 
  userForm.pageNo.value = pageNo;          
  userForm.action = url;    

  userForm.submit();           
}