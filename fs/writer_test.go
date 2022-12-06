package fs

import (
	"fmt"
	"math"
	"testing"

	"github.com/koykov/cbytecache"
)

var (
	testData = [][]byte{
		[]byte(`{"firstName":"John","lastName":"Smith","isAlive":true,"age":27,"address":{"streetAddress":"21 2nd Street","city":"New York","state":"NY","postalCode":"10021-3100"},"phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"},{"type":"mobile","number":"123 456-7890"}],"children":[],"spouse":null}`),
		[]byte(`{"$schema":"http://json-schema.org/schema#","title":"Product","type":"object","required":["id","name","price"],"properties":{"id":{"type":"number","description":"Product identifier"},"name":{"type":"string","description":"Name of the product"},"price":{"type":"number","minimum":0},"tags":{"type":"array","items":{"type":"string"}},"stock":{"type":"object","properties":{"warehouse":{"type":"number"},"retail":{"type":"number"}}}}}`),
		[]byte(`{"id":1,"name":"Foo","price":123,"tags":["Bar","Eek"],"stock":{"warehouse":300,"retail":20}}`),
		[]byte(`{"first name":"John","last name":"Smith","age":25,"address":{"street address":"21 2nd Street","city":"New York","state":"NY","postal code":"10021"},"phone numbers":[{"type":"home","number":"212 555-1234"},{"type":"fax","number":"646 555-4567"}],"sex":{"type":"male"}}`),
		[]byte(`{"fruit":"Apple","size":"Large","color":"Red"}`),
		[]byte(`{"quiz":{"sport":{"q1":{"question":"Which one is correct team name in NBA?","options":["New York Bulls","Los Angeles Kings","Golden State Warriros","Huston Rocket"],"answer":"Huston Rocket"}},"maths":{"q1":{"question":"5 + 7 = ?","options":["10","11","12","13"],"answer":"12"},"q2":{"question":"12 - 8 = ?","options":["1","2","3","4"],"answer":"4"}}}}`),
	}
	tdLen = len(testData)
)

func getTestBody(i int) []byte {
	return testData[i%tdLen]
}

func TestWriter(t *testing.T) {
	w := Writer{
		FilePath: "testdata/%Y-%m-%d--%H-%M-%S--%N.bin",
	}
	for i := 0; i < 100; i++ {
		body := getTestBody(i)
		e := cbytecache.Entry{
			Key:    fmt.Sprintf("key%d", i),
			Body:   body,
			Expire: math.MaxUint32,
		}
		_, err := w.Write(e)
		if err != nil {
			t.Error(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Error(err)
	}
}
