package primitives

import (
	"bufio"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var CexAddressSet map[Address]struct{}

func init() {
	CexAddressSet = make(map[Address]struct{})

	sourcePath := GetProjectRoot()
	cexFilePath := filepath.Join(sourcePath, "/primitives/cex.txt")

	file, err := os.Open(cexFilePath)
	if err != nil {
		log.Fatalf("cex.txt 파일을 열 수 없습니다: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		addrBytes, err := decodeHexAddress(line)
		if err != nil {
			log.Printf("주소 디코딩 실패: %s (%v)", line, err)
			continue
		}

		CexAddressSet[addrBytes] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("cex.txt 파일 읽기 오류: %v", err)
	}

	log.Printf("✅ CEX 주소 %d개 로드 완료\n", len(CexAddressSet))
}

func decodeHexAddress(s string) (Address, error) {
	var b Address

	// 0x 접두어 제거
	s = strings.ToLower(strings.TrimPrefix(s, "0x"))

	decoded, err := hex.DecodeString(s)
	if err != nil || len(decoded) != 20 {
		return b, err
	}
	copy(b[:], decoded)
	return b, nil
}

func SetCexAddress(addr Address) {
	CexAddressSet[addr] = struct{}{}
}

func IsCexAddress(addr Address) bool {
	_, exists := CexAddressSet[addr]
	return exists
}

type Address [20]byte

// ✅ 문자열 변환 (0x + hex encoding)
func (a Address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

type PredefinedAddress interface {
	MinerAddress |
		CexHotWalletAddress | CexColdWalletAddress |
		ERC20TokenAddress |
		LendingPoolAddress | LendingStakingTokenAddress |
		SwapLiquidityPoolAddress | LiquidityPoolStakingTokenAddress |
		RouterAddress |
		BeaconDepositAddress |
		NFTContractAddress |
		BridgeAddress
}

type DefinedOnProcess interface {
	UserEOA | CexDepositAddress
}

type UserEOA Address // 사용자 계정 (외부 소유 지갑)

type MinerAddress Address // 채굴자 주소 (마이닝 보상 수령 주소)

// Centralized Exchange (CEX)
type CexDepositAddress Address    // 거래소 입금주소 (사용자별 생성)
type CexHotWalletAddress Address  // 거래소 운영용 지갑 (빠른 출금용)
type CexColdWalletAddress Address // 거래소 장기 보관 지갑

type ERC20TokenAddress Address // ERC-20 토큰 컨트랙트 주소

// DeFi Lending/Staking 컨트랙트
type LendingPoolAddress Address         // Aave 등 Lending Pool 주소
type LendingStakingTokenAddress Address // Lending 예치 후 지급되는 이자토큰 (예: aETH, cETH 등)

// DeFi Swap 관련 컨트랙트
type SwapLiquidityPoolAddress Address         // Uniswap, SushiSwap 등 AMM 컨트랙트 주소
type LiquidityPoolStakingTokenAddress Address // LP토큰 컨트랙트 주소 (Uniswap LP토큰 등)

type RouterAddress Address // Uniswap, SushiSwap 등 라우터 컨트랙트 주소
// ETH 2.0 (Beacon Chain)
type BeaconDepositAddress Address // ETH2.0 스테이킹 주소

// NFT 관련
type NFTContractAddress Address // ERC-721, ERC-1155 컨트랙트 주소

// Bridge 관련
type BridgeAddress Address // Arbitrum, Optimism 등 브릿지 컨트랙트 주소

type UndefinedAddress Address // 정의되지 않은 주소

//! 다만 그럼에도 TokenAddress역시 중요함. 토큰에 tx시 토큰 transfer추적 가능
// *(0) 사용자->사용자 Eth 전송
// *(1) 사용자 → 사용자 토큰 전송
//User(EOA) --ERC20 Token 전송호출--> Token Contract → 수신자 EOA
// {
// 	"from": "User1 (EOA)",
// 	"to": "ERC20TokenContract",
// 	"value": "0",
// 	"input": "transfer(Receiver, Amount)"
//   }
//로그: ERC20의 Transfer 이벤트 발생:
// from: UserAddress
// to: ReceiverAddress
// amount: Amount

//*(2) 사용자 → ETH2.0 예치 & 회수 (Beacon chain staking)
// 예치
// {
// 	"from": "UserAddress",
// 	"to": "BeaconDepositContractAddress",
// 	"value": "32000000000000000000", //32 ETH
// 	"input": "staking validator data"
//   }
//
// 로그 (Deposit 이벤트)
// BeaconDepositContract에서 Deposit(address,uint256) 발생
//
// 회수
// {
// 	"from": "UserAddress",
// 	"to": "BeaconWithdrawalContract",
// 	"value": "0",
// 	"input": "withdraw()"
//   }
//
// 로그 (Transfer 이벤트)
// BeaconDepositContract → UserAddress로 ETH 이동

//*(3) 사용자 → LP(Liquidity Pool)에 예치 후 회수
// {
// 	"from": "UserAddress",
// 	"to": "LiquidityPoolAddress",
// 	"value": "1000000000000000000", // 1 ETH
// 	"input": ""
//   }
//
// 로그 발생:
// LiquidityPoolAddress에서 ETH 입금 확인 가능 (내부 처리됨)
// LP토큰을 UserAddress에 발행하는 Transfer 이벤트 발생:
// from: 0x000...0 (민팅)
// to: UserAddress
// amount: LP Token 수량
//

//회수
// {
// 	"from": "UserAddress",
// 	"to": "LiquidityPoolAddress",
// 	"value": "0",
// 	"input": "withdraw(LP Token Amount)"
//   }
//
// 로그 (2개 발생)
// LP토큰을 소각:
// from: UserAddress
// to: 0x000...0 (소각)
// ETH를 풀에서 사용자에게 전송:
// from: LiquidityPoolAddress
// to: UserAddress
// amount: ETH 수량
