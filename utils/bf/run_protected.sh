#!/bin/bash

# 실행 디렉토리로 이동
cd $(dirname "$0")

# 프로그램 시작
echo "프로그램 시작 중: $1"
"$@" &
PID=$!

echo "프로그램 PID: $PID"
sleep 1  # 프로세스가 제대로 시작되도록 잠시 대기

# PID 유효성 확인
if ps -p $PID > /dev/null; then
    echo "OOM 점수 설정 중..."
    sudo bash -c "echo -1000 > /proc/$PID/oom_score_adj"
    
    # 설정 확인
    oom_score=$(cat /proc/$PID/oom_score_adj)
    echo "OOM 점수 설정됨: $oom_score"
    
    # 프로세스 상태 유지
    echo "프로그램 실행 중... (종료하려면 Ctrl+C)"
    wait $PID
    echo "프로그램 종료됨 (종료 코드: $?)"
else
    echo "오류: 프로세스가 시작되지 않았습니다"
fi
