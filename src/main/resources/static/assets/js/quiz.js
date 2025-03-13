    document.addEventListener("DOMContentLoaded", function() {
      const questions = document.querySelectorAll(".question");
      let currentIndex = 0;
      
      // 초기: 첫 문항만 활성화
      questions.forEach((q, index) => {
        q.classList.toggle("active", index === currentIndex);
      });
      
      const prevBtn = document.getElementById("prevBtn");
      const nextBtn = document.getElementById("nextBtn");
      const submitBtn = document.getElementById("submitBtn");
      
      function updateButtons() {
        prevBtn.style.display = currentIndex === 0 ? "none" : "inline-block";
        nextBtn.style.display = currentIndex === questions.length - 1 ? "none" : "inline-block";
        submitBtn.style.display = currentIndex === questions.length - 1 ? "inline-block" : "none";
      }
      
      nextBtn.addEventListener("click", function() {
        if (currentIndex < questions.length - 1) {
          questions[currentIndex].classList.remove("active");
          currentIndex++;
          questions[currentIndex].classList.add("active");
          updateButtons();
          window.scrollTo(0, 0);
        }
      });
      
      prevBtn.addEventListener("click", function() {
        if (currentIndex > 0) {
          questions[currentIndex].classList.remove("active");
          currentIndex--;
          questions[currentIndex].classList.add("active");
          updateButtons();
          window.scrollTo(0, 0);
        }
      });
      
      updateButtons();
    });