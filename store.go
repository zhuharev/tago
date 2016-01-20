package tago

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/zhuharev/intarr"
)

var (
	TagoBucket      = []byte("tago")
	TagoTagsBucket  = []byte("tags")  // tago->tags
	TagoItemsBucket = []byte("items") // tago->items
)

type BoltStore struct {
	*bolt.DB
}

func NewBoltStore(db *bolt.DB) *BoltStore {
	return &BoltStore{DB: db}
}

func setTag(tx *bolt.Tx, tagName string, objPrefix []byte, objId int64) error {
	tagoBucket := tx.Bucket(TagoBucket)
	tagoTagsBucket := tagoBucket.Bucket(TagoTagsBucket)

	objBucket, e := tagoTagsBucket.CreateBucketIfNotExists(objPrefix)
	if e != nil {
		return e
	}

	sl, e := getItemsFromBucket(objBucket, tagName)
	if e != nil {
		return e
	}

	// todo overflow int32
	sl = append(sl, objId)

	bts, e := intarr.New(sl).Encode()
	if e != nil {
		return e
	}
	e = objBucket.Put([]byte(tagName), bts)

	return e
}

func getTagItems(tx *bolt.Tx, tagName string, objPrefix []byte) (arr []int64, e error) {
	var (
		tagoBucket     = tx.Bucket(TagoBucket)
		tagoTagsBucket = tagoBucket.Bucket(TagoTagsBucket)

		objBucket *bolt.Bucket
	)

	objBucket = tagoTagsBucket.Bucket(objPrefix)
	if objBucket == nil {
		e = fmt.Errorf("table for object not exist")
		return
	}

	arr, e = getItemsFromBucket(objBucket, tagName)
	return
}

func getItemsFromBucket(bucket *bolt.Bucket, tagName string) ([]int64, error) {
	bts := bucket.Get([]byte(tagName))

	sl, e := intarr.Decode(bts)
	if e != nil {
		return nil, e
	}

	return sl.Int64(), e
}

func removeTag(tx *bolt.Tx, tagName string, objPrefix []byte, objId int64) error {
	tagoBucket := tx.Bucket(TagoBucket)
	tagoTagsBucket := tagoBucket.Bucket(TagoTagsBucket)

	objBucket, e := tagoTagsBucket.CreateBucketIfNotExists(objPrefix)
	if e != nil {
		return e
	}

	arr, e := getItemsFromBucket(objBucket, tagName)
	if e != nil {
		return e
	}

	sl := intarr.New(arr)
	if sl.In(int32(objId)) {
		sl = sl.Remove(int32(objId))
	}

	bts, e := sl.Encode()
	if e != nil {
		return e
	}
	e = objBucket.Put([]byte(tagName), bts)

	return e
}
