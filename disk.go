package main

import (
	"os"
)

const (
	PAGE_SIZE = 4096
)

type PageID uint64

type DiskManager struct {
	HeapFile   *os.File
	NextPageID PageID
}

// New 新しいDiskManagerを作成
func New() *DiskManager {
	m := &DiskManager{}
	return m
}

// Open ファイルを開く
func (d *DiskManager) Open(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	d.HeapFile = f

	i, err := f.Stat()
	if err != nil {
		return err
	}

	d.NextPageID = PageID(i.Size() / PAGE_SIZE)

	return nil
}

// AllocatePage 新しいページIDを採番する
func (d *DiskManager) AllocatePage() PageID {
	d.NextPageID++
	return d.NextPageID
}

// Read ページデータを読み出す
func (d *DiskManager) Read(pageID PageID, data *[8]byte) error {
	offset := int64(PAGE_SIZE * uint64(pageID))
	_, err := d.HeapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = d.HeapFile.Read(data[:])
	if err != nil {
		return err
	}
	return nil
}

// Write ページデータに書き出す
func (d *DiskManager) Write(pageID PageID, data [8]byte) error {
	offset := int64(PAGE_SIZE * uint64(pageID))
	_, err := d.HeapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = d.HeapFile.Write(data[:])
	if err != nil {
		return err
	}
	return nil
}
