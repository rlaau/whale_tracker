package primitives

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/joho/godotenv"
)

// ✅ `init()`을 사용해 자동 실행
func init() {
	LoadEnv()
}

// ✅ Git 루트 디렉토리 찾기
func GetProjectRoot() string {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		log.Fatal("Git 루트 디렉토리를 찾을 수 없습니다. 프로젝트 루트 경로를 수동으로 설정하세요.")
	}
	return string(output[:len(output)-1]) // 개행 문자 제거
}
func LoadEnv() {
	projectRoot := GetProjectRoot()
	envLocalPath := filepath.Join(projectRoot, ".env.local")
	envExamplePath := filepath.Join(projectRoot, ".env.example")

	// .env.local이 있으면 로드, 없으면 .env.example 로드
	if _, err := os.Stat(envLocalPath); err == nil {
		err = godotenv.Load(envLocalPath)
		if err != nil {
			log.Fatalf(" Error loading .env.local file: %v", err)
		}
		log.Println("✅ Loaded .env.local")
	} else {
		err = godotenv.Load(envExamplePath)
		if err != nil {
			log.Fatalf("Error loading .env.example file: %v", err)
		}
		log.Println("⚠️ .env.local not found, loaded .env.example instead")
	}
}
