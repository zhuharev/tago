package tago

import (
	"github.com/boltdb/bolt"
)

type Tago struct {
	db *BoltStore
}

/*func New() (t *Tago, e error) {
	t = new(Tago)
	t.db = &memStore{}

	return
}
*/
func NewWithBoltDb(db *bolt.DB) (t *Tago, e error) {
	t = new(Tago)
	t.db = NewBoltStore(db)
	e = t.db.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucketIfNotExists(TagoBucket)
		if e != nil {
			return e
		}
		_, e = b.CreateBucketIfNotExists(TagoTagsBucket)
		if e != nil {
			return e
		}
		_, e = b.CreateBucketIfNotExists(TagoItemsBucket)
		if e != nil {
			return e
		}
		return nil
	})
	if e != nil {
		return
	}

	return
}

func (t *Tago) SetTag(tagName string, objPrefix []byte, objId int64) error {
	e := t.db.Update(func(tx *bolt.Tx) error {
		return setTag(tx, tagName, objPrefix, objId)
	})
	return e
}

func (t *Tago) GetTagItems(tagName string, objPrefix []byte) (res []int64, e error) {
	e = t.db.View(func(tx *bolt.Tx) error {
		res, e = getTagItems(tx, tagName, objPrefix)
		return e
	})
	return
}

func (t *Tago) RemoveTag(tagName string, objPrefix []byte, objId int64) error {
	e := t.db.Update(func(tx *bolt.Tx) error {
		return removeTag(tx, tagName, objPrefix, objId)
	})
	return e
}
