# ✅ 1. Go 공식 이미지 사용 (테스트 환경)
FROM golang:1.23.7 AS test

# ✅ 2. 필수 패키지 설치
RUN apt update && apt install -y \
    g++-12 gcc-12 clang make zsh curl \
    && rm -rf /var/lib/apt/lists/*

# ✅ 3. 작업 디렉토리 설정
WORKDIR /app

# ✅ 4. Go 환경 변수 설정
ENV GOPATH=/root/go \
    GOROOT=/usr/local/go \
    GOBIN=/root/go/bin \
    PATH="/root/go/bin:/usr/local/go/bin:$PATH" \
    CGO_ENABLED=1 \
    CC=gcc \
    CXX=clang++ \
    CXXFLAGS="-Wno-macro-redefined -Wno-unknown-warning-option" \
    CGO_LDFLAGS="-Wl,--allow-multiple-definition"

# ✅ 5. Go 모듈 파일 복사 및 의존성 설치
COPY go.mod go.sum ./
RUN go mod tidy && go mod download

# ✅ 6. 프로젝트 코드 복사
COPY . .

# ✅ 7. `go install` 실행 (캐싱하여 빌드 속도 개선)
RUN go install ./...

# ✅ 8. 실행 시 자동으로 환경 변수 로드 후 테스트 실행
CMD ["go", "test", "./...", "-coverprofile=coverage.out", "-v"]
