package repository_test

import (
	"context"
	"encoding/hex"
	"math/big"
	"testing"

	// 프로젝트 패키지
	"whale_tracker/adapters"
	"whale_tracker/primitives"
	"whale_tracker/repository"

	// mtest 관련
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// ✅ 단일 Insert 테스트
func TestMongoTransactionRepository_InsertTransaction(t *testing.T) {

	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	// 🔹 단일 Insert 시나리오 실행
	mt.Run("성공 케이스 - 단일 Insert", func(mt *mtest.T) {
		if mt.Coll == nil {
			t.Fatal("mt.Coll is nil, mtest initialization failed")
		}

		// 🔹 Repository 인스턴스에 가짜 Collection 주입
		testRepo := &repository.MongoTransactionRepository{
			Collection: mt.Coll, // mt.Coll을 직접 사용해야 함
		}

		// (1) InsertOne이 실행될 때, 성공 응답을 반환하도록 Mock 설정
		mt.AddMockResponses(mtest.CreateSuccessResponse())
		var txID [32]byte
		copy(txID[:], "asdnjkshbdhasbdhi1239814sdans") // 슬라이스로 변환 후 복사

		var from [20]byte
		copy(from[:], "0xSENDER")
		var to [20]byte
		copy(to[:], "0xRECEIVER")
		var x primitives.BigInt = primitives.BigInt{Int: big.NewInt(1234567890123456789)}

		// (2) 실제 메서드 호출
		err := testRepo.InsertTransaction(context.Background(), primitives.Transaction{
			TxID:  primitives.TxId(txID),
			From:  primitives.Address(from),
			To:    primitives.Address(to),
			Value: x,
		})

		// (3) 에러 확인
		if err != nil {
			t.Fatalf("InsertTransaction 실패: %v", err)
		}

		// (4) MongoDB 드라이버가 insert 커맨드를 실행했는지 검증
		started := mt.GetStartedEvent()
		if started == nil || started.CommandName != "insert" {
			t.Errorf("InsertOne 커맨드가 실행되지 않음")
		}
	})
}

// ✅ 테스트용 샘플 데이터 생성
func newTestTransaction(i int) primitives.Transaction {
	tx := primitives.Transaction{}

	// 32바이트 TxID
	copy(tx.TxID[:], hexDecode("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"))

	tx.TxSyntax = "TxUser2UserEthTransfer"
	tx.BlockNumber = uint64(i)

	// 20바이트 주소 (From, To)
	copy(tx.From[:], hexDecode("1122334455667788990011223344556677889900"))
	copy(tx.To[:], hexDecode("9988776655443322110099887766554433221100"))

	tx.Value = primitives.NewBigInt("1000000000000000000") // 1 ETH (Wei 단위)
	tx.GasLimit = primitives.NewBigInt("21000")
	tx.Input = "0xabcdef"

	return tx
}

// ✅ 헥스 문자열을 바이트 배열로 변환하는 유틸 함수
func hexDecode(s string) []byte {
	data, _ := hex.DecodeString(s)
	return data
}

// ✅ Find 테스트
func TestMongoTransactionRepository_FindTransactions(t *testing.T) {
	// 🔹 Mock 클라이언트 생성
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	// 🔹 Find 테스트
	mt.Run("성공 케이스 - 특정 결과 반환", func(mt *mtest.T) {
		if mt.Coll == nil {
			t.Fatal("mt.Coll is nil, mtest initialization failed")
		}
		testRepo := &repository.MongoTransactionRepository{
			Collection: mt.Coll, // Mock Collection 사용
		}
		// (1) 가짜 문서 데이터 (Cursor가 반환할 값)
		var txID1, txID2 [32]byte
		copy(txID1[:], hexDecode("aa"))
		copy(txID2[:], hexDecode("bb"))

		var from1, from2 [20]byte
		copy(from1[:], "0xFROM1")
		copy(from2[:], "0xFROM2")

		var to1, to2 [20]byte
		copy(to1[:], "0xTO1")
		copy(to2[:], "0xTO2")

		value1 := big.NewInt(1000000000000000000) // 1 ETH in wei
		value2 := big.NewInt(2000000000000000000) // 2 ETH in wei

		firstTx := adapters.NewMongoTransaction(primitives.Transaction{
			TxID:  primitives.TxId(txID1),
			From:  primitives.Address(from1),
			To:    primitives.Address(to1),
			Value: primitives.BigInt{Int: value1},
		})

		secondTx := adapters.NewMongoTransaction(primitives.Transaction{
			TxID:  primitives.TxId(txID2),
			From:  primitives.Address(from2),
			To:    primitives.Address(to2),
			Value: primitives.BigInt{Int: value2},
		})

		// ✅ 구조체 → BSON 변환
		var firstDoc, secondDoc bson.D
		bsonBytes1, _ := bson.Marshal(firstTx) // Go struct → BSON 변환
		bson.Unmarshal(bsonBytes1, &firstDoc)  // BSON → bson.D 변환

		bsonBytes2, _ := bson.Marshal(secondTx) // Go struct → BSON 변환
		bson.Unmarshal(bsonBytes2, &secondDoc)  // BSON → bson.D 변환

		// (2) Find 커맨드가 실행될 때 반환할 Cursor 응답 생성
		mockCursor := mtest.CreateCursorResponse(2, "blockchain.transactions", mtest.FirstBatch, firstDoc, secondDoc)

		// ✅ 커서가 끝났음을 나타내는 응답 (빈 batch 사용)
		closeCursor := mtest.CreateCursorResponse(0, "blockchain.transactions", mtest.NextBatch)

		// (3) 응답 설정 (Find 커맨드 실행 시 Mock 데이터 반환)
		mt.AddMockResponses(mockCursor, closeCursor)

		// (4) FindTransactions 실행
		filter := bson.M{"from": from1[:]} // `from` 필드로 필터링
		gotTxs, err := testRepo.FindTransactions(context.Background(), filter, 10)

		// (5) 에러 확인
		if err != nil {
			t.Fatalf("FindTransactions 실패: %v", err)
		}

		// (6) 반환된 데이터 검증
		if len(gotTxs) != 2 {
			t.Errorf("Find 결과 개수 기대: 2, 실제: %d", len(gotTxs))
		}

		// (7) 개별 트랜잭션 데이터 검증
		if len(gotTxs) > 0 {
			expectedHash := primitives.TxId(txID1).String()
			actualHash := gotTxs[0].TxID.String() // [32]byte → string 변환
			if actualHash != expectedHash {
				t.Errorf("첫 번째 트랜잭션 TxID 불일치. 기대값: %s, 실제값: %s", expectedHash, actualHash)
			}

			expectedValue := value1.String()
			actualValue := gotTxs[0].Value.String()
			if actualValue != expectedValue {
				t.Errorf("첫 번째 트랜잭션 Value 불일치. 기대값: %s, 실제값: %s", expectedValue, actualValue)
			}
		}
	})
}
