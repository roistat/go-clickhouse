package clickhouse

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"

	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type TestHandler struct {
	Result string
}

func (h *TestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/tab-separated-values; charset=UTF-8")
	fmt.Fprint(w, h.Result)
}

func TestExec(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := HttpTransport{}
	conn := Conn{Host: server.URL, transport: transport}
	q := NewQuery("SELECT * FROM testdata")
	resp, err := transport.Exec(&conn, q, false)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)

}

func TestExecReadOnly(t *testing.T) {
	handler := &TestHandler{Result: "1  2.5 clickid68235\n2 -0.14   clickidsdkjhj44"}
	server := httptest.NewServer(handler)
	defer server.Close()

	transport := HttpTransport{}
	conn := Conn{Host: server.URL, transport: transport}
	q := NewQuery(url.QueryEscape("SELECT * FROM testdata"))
	query := prepareHttp(q.Stmt, q.args)
	query = "?query=" + url.QueryEscape(query)
	resp, err := transport.Exec(&conn, q, true)
	assert.Equal(t, nil, err)
	assert.Equal(t, handler.Result, resp)

}

func TestPrepareHttp(t *testing.T) {
	p := prepareHttp("SELECT * FROM table WHERE key = ?", []interface{}{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func TestPrepareHttpArray(t *testing.T) {
	p := prepareHttp("INSERT INTO table (arr) VALUES (?)", Row{Array{"val1", "val2"}})
	assert.Equal(t, "INSERT INTO table (arr) VALUES (['val1','val2'])", p)
}

func TestPrepareExecPostRequest(t *testing.T) {
	q := NewQuery("SELECT * FROM testdata")
	req, err := prepareExecPostRequest("127.0.0.0:8123", q)
	assert.Equal(t, nil, err)
	data, err := ioutil.ReadAll(req.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, "SELECT * FROM testdata", string(data))
}

func TestPrepareExecPostRequestWithExternalData(t *testing.T) {
	q := NewQuery("SELECT * FROM testdata")
	q.AddExternal("data1", "ID String, Num UInt32", []byte("Hello\t22\nHi\t44"))
	q.AddExternal("extdata", "Num UInt32, Name String", []byte("1	first\n2	second"))

	req, err := prepareExecPostRequest("127.0.0.0:8123", q)
	assert.Equal(t, nil, err)
	assert.Equal(t, "SELECT * FROM testdata", req.URL.Query().Get("query"))
	assert.Equal(t, "ID String, Num UInt32", req.URL.Query().Get("data1_structure"))
	assert.Equal(t, "Num UInt32, Name String", req.URL.Query().Get("extdata_structure"))

	mediaType, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	assert.Equal(t, nil, err)
	assert.Equal(t, true, strings.HasPrefix(mediaType, "multipart/"))

	reader := multipart.NewReader(req.Body, params["boundary"])

	p, err := reader.NextPart()
	assert.Equal(t, nil, err)

	data, err := ioutil.ReadAll(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, "Hello\t22\nHi\t44", string(data))

	p, err = reader.NextPart()
	assert.Equal(t, nil, err)

	data, err = ioutil.ReadAll(p)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1\tfirst\n2\tsecond", string(data))
}

func BenchmarkPrepareHttp(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 8000)
	for i := 0; i < 8000; i++ {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttp("INSERT INTO t VALUES "+params, args)
	}
}
