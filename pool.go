package main

import "errors"

var (
	ErrNotFreeBuffer = errors.New("not free buffer")
)

type (
	Page     [PAGE_SIZE]byte
	BufferID int64
)

// Buffer バッファプールのバッファ定義
// ヒープファイルから読み取ったデータを保持するデータ構造
type Buffer struct {
	PageID  PageID // バッファプールのページID
	Page    Page   // バッファプールのページデータが保存される
	IsDirty bool   // 変更があったことを示すフラグ
}

// Frame バッファプールのフレーム定義
type Frame struct {
	UsageCount uint64 // バッファの利用した回数
	RefCount   uint64 // バッファの参照中の数
	Buffer     Buffer
}

// BufferPool バッファプールの定義
type BufferPool struct {
	Buffers      []Frame
	NextVictimID BufferID
}

// Size バッファのサイズ
func (bp *BufferPool) Size() int {
	return len(bp.Buffers)
}

// Evict 破棄、もしくは使用可能なバッファIDを取得
// 一度も使用されたことがないバッファが存在した場合そのIDが返る
// すべてが貸し出し中の場合は -1 が返る
func (bp *BufferPool) Evict() BufferID {
	var consecutivePinned int
	poolSize := bp.Size()
	var nextVictimID BufferID

	// clock-sweepにより、参照されていないバッファの中から、
	// 利用回数が少ないものから破棄・再利用する
	// 詳細は https://www.interdb.jp/pg/pgsql08.html#_8.4.4.
	for {
		nextVictimID = bp.NextVictimID
		frame := bp.Buffers[nextVictimID]

		// バッファの利用回数が0の場合はそのIDを返す
		if frame.UsageCount == 0 {
			break
		}

		// 参照中じゃなければ、使用回数をデクリメント
		if frame.RefCount == 0 {
			frame.UsageCount--
			consecutivePinned = 0
		} else {
			consecutivePinned++
			// すべてのバッファが使われている場合は -1 を返す
			if consecutivePinned >= poolSize {
				return -1
			}
		}

		// インクリメント
		bp.NextVictimID = (bp.NextVictimID + 1) % BufferID(poolSize)
	}

	return nextVictimID
}

// BufferPollManager バッファプールを管理
type BufferPollManager struct {
	Disk      DiskManager
	Pool      BufferPool
	PageTable map[PageID]BufferID // ページIDからバッファIDを特定するためのmap
}

func (bpm *BufferPollManager) FetchPage(pageID PageID) (*Buffer, error) {
	bufferID, ok := bpm.PageTable[pageID]
	if ok {
		frame := bpm.Pool.Buffers[bufferID]
		frame.UsageCount++
		return &frame.Buffer, nil
	}

	bufferID = bpm.Pool.Evict()
	if bufferID == -1 {
		return nil, ErrNotFreeBuffer
	}
	frame := bpm.Pool.Buffers[bufferID]
	buffer := frame.Buffer
	evictPageID := frame.Buffer.PageID

	// バッファに変更があった場合は中身をヒープファイルに書き出す
	if buffer.IsDirty {
		if err := bpm.Disk.Write(buffer.PageID, buffer.Page); err != nil {
			return nil, err
		}
	}
	buffer.PageID = pageID
	buffer.IsDirty = false

	bpm.Disk.Read(pageID, buffer.Page)
	frame.UsageCount = 1

	delete(bpm.PageTable, evictPageID)
	bpm.PageTable[pageID] = bufferID

	return &buffer, nil
}
