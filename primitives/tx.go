package primitives

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
)

type TxId [32]byte

func (t TxId) String() string {
	return "0x" + hex.EncodeToString(t[:])
}

func (t TxId) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t *TxId) UnmarshalJSON(data []byte) error {
	var hexStr string
	if err := json.Unmarshal(data, &hexStr); err != nil {
		return err
	}
	bytes, err := hex.DecodeString(hexStr[2:]) // "0x" 제거
	if err != nil {
		return err
	}
	copy(t[:], bytes)
	return nil
}

// ✅ BigInt 변환 (MongoDB & BigQuery 호환)
type BigInt struct {
	*big.Int
}

// ✅ JSON 변환 (BigInt → string)
func (b BigInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.String())
}

// ✅ JSON 복구 (string → BigInt)
func (b *BigInt) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	b.Int = new(big.Int)
	b.Int.SetString(str, 10) // 10진수 변환
	return nil
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

// ✅ 트랜잭션 구조체 (MongoDB & BigQuery 호환)
type Transaction struct {
	TxID        TxId    `bson:"tx_id" bigquery:"tx_id"`
	TxSyntax    string  `bson:"tx_syntax" bigquery:"tx_syntax"`
	BlockNumber uint64  `bson:"block_number" bigquery:"block_number"`
	From        Address `bson:"from" bigquery:"from"`
	To          Address `bson:"to" bigquery:"to"`
	Value       BigInt  `bson:"value" bigquery:"value"`
	GasLimit    BigInt  `bson:"gas_limit" bigquery:"gas_limit"`
	Input       string  `bson:"input" bigquery:"input"`
}
