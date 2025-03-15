package primitives

import (
	"encoding/hex"
	"encoding/json"
)

type Address [20]byte

func (a Address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

func (a Address) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a *Address) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return err
	}
	bytes, err := hex.DecodeString(hexStr[2:]) // "0x" 제거
	if err != nil {
		return err
	}
	copy(a[:], bytes)
	return nil
}

type UserEOA Address // 사용자 계정 (외부 소유 지갑)

// Centralized Exchange (CEX)
type CexDepositAddress Address    // 거래소 입금주소 (사용자별 생성)
type CexHotWalletAddress Address  // 거래소 운영용 지갑 (빠른 출금용)
type CexColdWalletAddress Address // 거래소 장기 보관 지갑

// DeFi Lending/Staking 컨트랙트
type LendingPoolAddress Address         // Aave 등 Lending Pool 주소
type LendingStakingTokenAddress Address // Lending 예치 후 지급되는 이자토큰 (예: aETH, cETH 등)

// DeFi Swap 관련 컨트랙트
type SwapLiquidityPoolAddress Address         // Uniswap, SushiSwap 등 AMM 컨트랙트 주소
type LiquidityPoolStakingTokenAddress Address // LP토큰 컨트랙트 주소 (Uniswap LP토큰 등)

// ETH 2.0 (Beacon Chain)
type BeaconDepositAddress Address // ETH2.0 스테이킹 주소

// NFT 관련
type NFTContractAddress Address // ERC-721, ERC-1155 컨트랙트 주소

// Bridge 관련
type BridgeAddress Address // Arbitrum, Optimism 등 브릿지 컨트랙트 주소

type UndefinedAddress Address // 정의되지 않은 주소

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
