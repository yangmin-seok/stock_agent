var canvas = document.getElementById('canvs');
var ctx = canvas.getContext('2d'); // 도구

canvas.width = window.innerWidth - 100;
canvas.height = window.innerHeight - 100;

var img = new Image();
img.src = 'dino.png';

var dino = {
    x : 10,
    y : 200, 
    width : 50,
    height : 50,
    draw(){
        ctx.fillStyle = 'green';
        //ctx.fillRect(this.x, this.y, this.width, this.height);
        ctx.drawImage(img, this.x, this.y, this.width, this.height);
    }
} // 객체 생성
//dino.draw();


class Cactus {
    constructor(){
        this.x = 500;
        this.y = 200;
        this.width = 50;
        this.height = 50;
    }
    draw(){
        ctx.fillStyle = 'red';
        //ctx.fillRect(this.x, this.y, this.width, this.height);
        ctx.drawImage(img1, this.x, this.y, this.width, this.height);
    }
}

var img1 = new Image();
img1.src = 'cactus.png';
//var cactus = new Cactus();
//cactus.draw();

var timer = 0;
var jumptimer = 0;
var cactusArr = []; // 장애물 배열
var animation;

// 공룡을 x축으로 10만큼 이동, 1초에 60번 x++ 
function gameLoop(){
    animation = requestAnimationFrame(gameLoop); // 비동기적으로 실행, 그리기 일정에만 등록
    timer++;

    // 캔버스 초기화
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // cactus 2~3초 마다 하나씩 생성
    if (timer % 200 === 0){
        var cactus = new Cactus();
        cactusArr.push(cactus);
    }
        
    // 장애물 그리기
    cactusArr.forEach((a, i, o) => {
        // 좌표가 0 미만이면 배열에서 제거
        if (a.x < 0){
            o.splice(i, 1); // 배열에서 i번째 요소 1개 제거
        }
        a.x--;
        checkCollision(dino, a);
        a.draw(); // (current value, index, array)
    })

    // 스페이스바 누르면 공룡이 점프
    if (isJumping === true){
        dino.y--;
        jumptimer++;
    }
    if (isJumping === false){
        if (dino.y < 200){
            dino.y++;
        }
    } 
    if (jumptimer > 100){
        isJumping = false;
        jumptimer = 0;
    }
    dino.draw();
}
gameLoop();

// 충돌 확인
// dino의 오른쪽 x좌표와, catus의 왼쪽 x좌표가 겹치면 stop
// dino의 아래 y좌표와 , cactus의 위쪽 y좌표가 겹치면 stop
function checkCollision(dino, cactus){
    var xdiff = cactus.x - (dino.x + dino.width);
    var ydiff = cactus.y - (dino.y + dino.height);

    if (xdiff < 0 && ydiff < 0){
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        cancelAnimationFrame(animation); // callback 함수의 등록을 취소
    }
}

var isJumping = false;
document.addEventListener('keydown', function(e){
    if (e.code === 'Space'){
        isJumping = true;
    }
});
