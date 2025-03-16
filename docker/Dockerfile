# ✅ 1. Go 빌드 스테이지
FROM golang:1.23.7 AS builder

# ✅ 2. 필수 패키지 설치
RUN apt update && apt install -y \
    g++-12 gcc-12 clang make zsh curl \
    && rm -rf /var/lib/apt/lists/*

# ✅ 3. 작업 디렉토리 설정
WORKDIR /app

# ✅ 4. Go 환경 변수 설정 (빌드 캐시 활용)
ENV GOPATH=/root/go \
    GOROOT=/usr/local/go \
    GOBIN=/root/go/bin \
    PATH="/root/go/bin:/usr/local/go/bin:$PATH" \
    CGO_ENABLED=1 \
    CC=gcc \
    CXX=clang++ \
    CXXFLAGS="-Wno-macro-redefined -Wno-unknown-warning-option" \
    CGO_LDFLAGS="-Wl,--allow-multiple-definition" \
    GOMODCACHE=/root/go/pkg/mod \
    GOCACHE=/root/.cache/go-build

# ✅ 5. 표준 라이브러리(`std`) 미리 빌드하여 캐싱
RUN go install -v std

# ✅ 6. Go 모듈 파일만 먼저 복사하여 의존성 캐시 유지
COPY go.mod go.sum ./
RUN go mod tidy && go mod download
RUN go mod verify

# ✅ 7. 미리 무거운 패키지 빌드하여 캐싱 최적화
RUN go get -v \
    github.com/mattn/go-sqlite3 \
    github.com/goccy/go-zetasqlite \
    github.com/stretchr/testify/assert \
    go.mongodb.org/mongo-driver/bson \
    go.mongodb.org/mongo-driver/mongo/integration/mtest \
    cel.dev/expr \
    github.com/felixge/httpsnoop

# ✅ 8. 특정 무거운 라이브러리 미리 빌드하여 캐싱 최적화
RUN go build -v -x -o /dev/null \
    github.com/mattn/go-sqlite3 \
    github.com/goccy/go-zetasqlite \
    github.com/stretchr/testify/assert \
    go.mongodb.org/mongo-driver/bson \
    go.mongodb.org/mongo-driver/mongo/integration/mtest \
    cel.dev/expr \
    github.com/felixge/httpsnoop

# ✅ 9. 의존성 기반으로 미리 `go install` 실행 (코드 없이 설치)
RUN go install ./...

# ✅ 10. 프로젝트 코드 복사 (캐시 유지됨)
COPY . ./

# ✅ 11. 프로젝트 빌드 (Go 바이너리 생성)
RUN go build -o whale_tracker main.go

# ✅ 12. 실행용 이미지 (작은 Debian 사용)
FROM debian:stable-slim AS final

# ✅ 13. 필수 패키지 설치 (CA 인증서 추가)
RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# ✅ 14. 실행 파일 복사
WORKDIR /app
COPY --from=builder /app/whale_tracker .

# ✅ 15. 컨테이너 실행 시 실행할 명령어
CMD ["./whale_tracker"]
