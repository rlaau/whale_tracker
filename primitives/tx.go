package primitives

import "math/big"

type TxId [32]byte
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

// 트랜잭션 리시트 구조체
type Transaction struct {
	TxID        TxId // 트랜잭션 해시
	TxSyntax    TxSyntax
	BlockNumber BlockNumber // 포함된 블록 번호

	From     Address // 보낸 주소
	To       Address // 받는 주소
	Value    big.Int // 전송된 ETH (wei 단위)
	GasLimit big.Int // 가스 제한

	//From->To에 대한 설명 및 그 부수효과
	//Eth전송은 부수효과가 없음-> 로그 없음
	//토큰 전송의 경우 각종 부수효과 기록됨
	//즉, 토큰 전송의 경우
	//From User, TO TokenContract가 끝인데
	//그 부수효과로
	// Transfer(user,recepient,amount) 이벤트가 발생
	//이 이벤트는 로그에 기록됨
	input string // 입력 데이터

}
