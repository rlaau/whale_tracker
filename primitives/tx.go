package primitives

import (
	"encoding/hex"
	"math/big"
)

// 트랜잭션 구조체
type Transaction struct {
	TxID        TxId
	TxSyntax    string
	BlockNumber uint64
	From        Address
	To          Address
	Value       BigInt
	GasLimit    BigInt
	Input       string
}

// TX 원시 데이터
// {
// 	"hash": "0x...",       // 트랜잭션 해시
// 	"from": "0xSender",    // 발신 주소
// 	"to": "0xRecipient",   // 수신 주소 (EOA or CA)
// 	"value": "0",          // 전송 ETH 양
// 	"gas": 21000,          // 가스 제한
// 	"gasPrice": "50 Gwei", // 가스 가격
// 	"input": "0x..."       // 호출 데이터 (ABI encoded data)(함수 시그니처 및 파라미터)
// Eth전송만 할 시엔 input값이 없음. (물론, 스테이킹 시도 이더 전송 취급이라, 이걸 바탕으로 신택스 확정은 불가)
//   }

type TxId [32]byte

// ✅ 문자열 변환 (0x + hex encoding)
func (t TxId) String() string {
	return "0x" + hex.EncodeToString(t[:])
}

// ✅ BigInt 변환 (MongoDB & BigQuery 호환)
type BigInt struct {
	Int *big.Int
}

// ✅ BigInt 생성자 함수
func NewBigInt(value string) BigInt {
	b := new(big.Int)
	b.SetString(value, 10)
	return BigInt{Int: b}
}

// ✅ 문자열 변환 (MongoDB & BigQuery에서 사용)
func (b BigInt) String() string {
	if b.Int == nil {
		return "0"
	}
	return b.Int.String()
}
func (b *BigInt) SetString(value string, base int) {
	if b.Int == nil {
		b.Int = new(big.Int)
	}
	b.Int.SetString(value, base)
}

type BlockNumber uint64

type TxSyntax uint8

const (
	TxUser2CexDeposit TxSyntax = iota // 사용자->거래소 예치계좌 입금

	//입금 시에도, 회수 시에도, 결국은 LP나 SW의 리퀴디티 풀만 알면 됨
	//Tx는 거기에만 보내면, 알아서 tx부수효과로 LP,Sw등이 transfer이벤트를 발생시킴
	TxUser2LpStaking  // 사용자->LP 스테이킹
	TxUser2LpWithdraw // 사용자->LP 출금

	TxUser2SwStaking  // 사용자->스왑 스테이킹
	TxUser2SwWithdraw // 사용자->스왑 출금

	TxUser2Swap // 사용자->스왑

	TxUser2BcStaking  // 사용자->비컨 스테이킹
	TxUser2BcWithdraw // 사용자->비컨 출금

	TxUser2BridgeDeposit  // 사용자->브릿지 입금
	TxUser2BridgeWithdraw // 사용자->브릿지 출금

	TxUser2UserTokenTransfer // 사용자->사용자 토큰 전송
	TxUser2UserEthTransfer   // 사용자->사용자 Eth 전송

	TxUser2NftMinting // 사용자->NFT 발행

	TxCexDeposit2User         // 거래소 예치계좌->사용자 출금
	TxCexDeposit2CexHotWallet // 거래소 예치계좌->거래소 핫월렛

	TxUndefined // 정의되지 않은 트랜잭션

)
