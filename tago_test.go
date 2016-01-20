package tago

import (
	"github.com/boltdb/bolt"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

var (
	testDbPath = "ale"
)

func clean() {
	os.RemoveAll(testDbPath)
}

func TestTag(t *testing.T) {
	defer clean()
	db, e := bolt.Open(testDbPath, 0666, nil)
	if e != nil {
		panic(e)
	}
	tPrefix := []byte("da")
	Convey("test tago", t, func() {

		Convey("store", func() {
			t, e := NewWithBoltDb(db)
			So(t, ShouldNotBeNil)

			e = t.SetTag("ale", tPrefix, 1)
			So(e, ShouldBeNil)

			res, e := t.GetTagItems("ale", tPrefix)
			So(len(res), ShouldEqual, 1)
			So(e, ShouldBeNil)

			e = t.RemoveTag("ale", tPrefix, 1)
			So(e, ShouldBeNil)

			res, e = t.GetTagItems("ale", tPrefix)
			So(len(res), ShouldEqual, 0)
			So(e, ShouldBeNil)

		})
	})
}
