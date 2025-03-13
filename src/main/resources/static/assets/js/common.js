/*
 * Utile function 
 * 2024/12/19 
 */

/**
 * 입력 값이 비어 있는 지 확인 하는 함수
 * @param {*} value 
 * @returns true(비어 있음)/false
 */
let isEmpty = function(value){
  if(null === value || value == undefined){
    return true;
  }

  //문자열 그리고 ''
  if(typeof value ==='string' && value.trim() === ''){
    return true;
  }

  return false;
}