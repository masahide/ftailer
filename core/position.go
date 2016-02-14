package core

import (
	"fmt"
	"time"
)

type Position struct {
	Name       string    `json:"n,omitempty"`
	CreateAt   time.Time `json:"ct,omitempty"`
	Offset     int64     `json:"o,omitempty"`
	HeadHash   string    `json:"h,omitempty"`
	HashLength int64     `json:"hl,omitempty"`
}

func (p Position) String() string {
	return fmt.Sprintf("Name:%s, CreateAt:%s, Offset:%d, HeadHash:%s, hashLen:%d", p.Name, p.CreateAt, p.Offset, p.HeadHash, p.HashLength)

}

func GetPositon(db *FtailDB) (Position, error) {
	var p Position
	if db.PosError != nil {
		return p, db.PosError
	}
	if db.Pos == nil {
		return Position{}, nil
	}
	return *db.Pos, nil
}
