package main

import (
	"bytes"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"html/template"
	"io"
	"io/fs"
	"log"
	"maps"
	"net/http"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

const timeFormat = "2006-01-02_15:04:05"

var (
	//go:embed templates/index.html
	indexTemplateHtml string

	indexTemplate = template.Must(template.New("index").Parse(indexTemplateHtml))
)

// dataStore stores the data for the server.
type dataStorage struct {
	db *sql.DB
}

func newDataStorage(dataPath string) (*dataStorage, error) {
	dbPath := path.Join(dataPath, "database.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %q: %v", dbPath, err)
	}

	createSensors := `
	CREATE TABLE IF NOT EXISTS sensors (
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT
	);
	`
	if _, err := db.Exec(createSensors); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create sensors table: %v", err)
	}

	createData := `
	CREATE TABLE IF NOT EXISTS measurements (
		sensor_id INTEGER,
		timestamp DATETIME,
		temperature FLOAT,
		pressure FLOAT,
		humidity FLOAT
	);
	`
	if _, err := db.Exec(createData); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create measurements table: %v", err)
	}

	return &dataStorage{
		db: db,
	}, nil
}

func (d *dataStorage) Close() {
	d.db.Close()
}

// Return the list of available sensors.
func (d *dataStorage) Sensors() map[string]int64 {
	query := `SELECT id, name FROM sensors;`
	rows, err := d.db.Query(query)
	if err != nil {
		// TODO: decide how to handle the error.
		log.Fatal(err)
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		result[name] = id
	}
	return result
}

func (d *dataStorage) ReadData(sensor string, startDate, endDate time.Time) (Data, error) {
	sensors := d.Sensors()
	sensorID, ok := sensors[sensor]
	if !ok {
		return Data{}, fmt.Errorf("sensor %q is not found", sensor)
	}
	query := `SELECT
	timestamp, temperature, pressure, humidity
	FROM measurements
	WHERE sensor_id = ? AND timestamp >= ? AND timestamp <= ?
	ORDER BY timestamp;`
	rows, err := d.db.Query(query, sensorID, startDate, endDate)
	if err != nil {
		return Data{}, fmt.Errorf("failed to read data for sensor %q: %v", sensor, err)
	}
	return Data{
		rows:  rows,
		empty: !rows.Next(),
	}, nil
}

func (d *dataStorage) StoreRecord(sensor string, rd Record) error {
	sensors := d.Sensors()
	sensorID, ok := sensors[sensor]
	if !ok {
		// Add a new sensor.
		// TODO: locking!
		res, err := d.db.Exec(`INSERT INTO sensors (name) VALUES (?);`, sensor)
		if err != nil {
			return fmt.Errorf("failed to insert sensor %q: %v", sensor, err)
		}
		if sensorID, err = res.LastInsertId(); err != nil {
			// The last insert id is not supported.
			// Get the sensors again.
			sensors = d.Sensors()
			sensorID, ok = sensors[sensor]
			if !ok {
				return fmt.Errorf("sensor %q is not found after insert", sensor)
			}
		}
	}
	_, err := d.db.Exec(`INSERT INTO measurements (sensor_id, timestamp, temperature, pressure, humidity)
	VALUES (?,?,?,?,?);`, sensorID, rd.Time, rd.Temperature, rd.Pressure, rd.Humidity)
	return err
}

type Data struct {
	rows  *sql.Rows
	empty bool
}

func (d Data) IsEmpty() bool {
	return d.empty
}

func (d Data) Write(w http.ResponseWriter) error {
	// Note: the first Next() is already called at the time of creation.
	buf := &bytes.Buffer{}
	buf.WriteString("#time,temperature,pressure,humidity\n")
	count := 0
	for {
		count++
		var rd Record
		if err := d.rows.Scan(&rd.Time, &rd.Temperature, &rd.Pressure, &rd.Humidity); err != nil {
			return err
		}
		if _, err := buf.WriteString(rd.String() + "\n"); err != nil {
			return err
		}
		if !d.rows.Next() {
			break
		}
	}
	log.Printf("written %d rows", count)
	w.Write(buf.Bytes())
	return nil
}

func (d Data) Close() {
	if d.rows != nil {
		d.rows.Close()
	}
}

type reqHandler struct {
	store    *dataStorage
	staticFS fs.FS
	username string
	password string
}

func NewReqHandler(store *dataStorage, staticPath, username, password string) (*reqHandler, error) {
	return &reqHandler{
		store:    store,
		staticFS: os.DirFS(staticPath),
		username: username,
		password: password,
	}, nil
}

func (d *reqHandler) ServeMux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /{$}", d.handleIndex)
	m.HandleFunc("GET /index.html", d.handleIndex)
	m.HandleFunc("GET /static/{objectname...}", d.handleStatic)
	m.HandleFunc("GET /data/{sensor}", d.handleData)
	m.HandleFunc("POST /upload/{sensor}", basicAuth(d.postData, d.username, d.password))
	return m
}

func (d reqHandler) handleIndex(w http.ResponseWriter, r *http.Request) {
	log.Printf("handling index")
	w.WriteHeader(http.StatusOK)
	indexTemplate.Execute(w, map[string]any{
		"Sensors": slices.Collect(maps.Keys(d.store.Sensors())),
	})
}

func (d reqHandler) handleStatic(w http.ResponseWriter, r *http.Request) {
	fn := r.PathValue("objectname")
	log.Printf("handling static %q", fn)
	handleFS(w, fn, d.staticFS, "static")
}

func parseReqDates(r *http.Request) (time.Time, time.Time, error) {
	endDate := time.Now()
	if err := r.ParseForm(); err != nil {
		return endDate, endDate, fmt.Errorf("failed to parse request: %v", err)
	}

	if ed := r.Form.Get("enddate"); ed != "" {
		et, err := time.Parse(time.DateOnly, ed)
		if err != nil {
			return endDate, endDate, fmt.Errorf("failed to parse enddate %q: %v", ed, err)
		}
		endDate = et
		log.Printf("enddate is provided and is: %q", endDate)
	} else {
		log.Printf("enddate is now: %q", endDate)
	}

	days := 7
	if inDays := r.Form.Get("days"); inDays != "" {
		v, err := strconv.Atoi(inDays)
		if err != nil {
			return endDate, endDate, fmt.Errorf("failed to parse days %q: %v", inDays, err)
		}
		if v <= 0 {
			return endDate, endDate, fmt.Errorf("parameter days must be positive, got %q", inDays)
		}
		days = v
	}
	return endDate.Add(-time.Duration(days) * time.Hour * 24), endDate, nil
}

func httpError(w http.ResponseWriter, msg string, code int) {
	log.Printf("HTTP error %d: %s", code, msg)
	http.Error(w, msg, code)
}

func (d reqHandler) handleData(w http.ResponseWriter, r *http.Request) {
	log.Printf("handling data %q, uri=%q", r.PathValue("sensor"), r.RequestURI)
	startDate, endDate, err := parseReqDates(r)
	if err != nil {
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	sensor := r.PathValue("sensor")
	log.Printf("sensor %s, start %s, end %s", sensor, startDate, endDate)
	if _, ok := d.store.Sensors()[sensor]; !ok {
		httpError(w, fmt.Sprintf("unknown sensor %q", sensor), http.StatusBadRequest)
		return
	}
	data, err := d.store.ReadData(sensor, startDate, endDate)
	if err != nil {
		httpError(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	if data.IsEmpty() {
		httpError(w, fmt.Sprintf("no data found for sensor %q in the range of [%s..%s]", sensor, startDate, endDate), http.StatusNotFound)
		return
	}
	w.Header().Set("content-type", "text/csv")
	w.WriteHeader(http.StatusOK)
	if err := data.Write(w); err != nil {
		log.Printf("failed to write: %v", err)
	}
}

func basicAuth(next http.HandlerFunc, user, pass string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		unauthorized := func(w http.ResponseWriter) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}

		authHeader := r.Header.Get("Authorization")
		s := strings.SplitN(authHeader, " ", 2)
		if len(s) != 2 || s[0] != "Basic" {
			log.Printf("bad authorization header %q", authHeader)
			unauthorized(w)
			return
		}

		decoded, err := base64.StdEncoding.DecodeString(s[1])
		if err != nil {
			log.Printf("cannot decode auth header: %v", err)
			unauthorized(w)
			return
		}

		toMatch := user + ":" + pass

		if subtle.ConstantTimeCompare([]byte(decoded), []byte(toMatch)) != 1 {
			log.Printf("wrong user/pass %q", decoded)
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	}
}

type Record struct {
	Time        time.Time `json:",omitempty,format:'2006-01-02_15:04:05'"`
	Temperature float64   `json:"temperature,omitempty"`
	Pressure    float64   `json:"pressure,omitempty"`
	Humidity    float64   `json:"humidity,omitempty"`
}

func (r Record) String() string {
	return fmt.Sprintf("%s,%v,%v,%v", r.Time.Format(timeFormat), r.Temperature, r.Pressure, r.Humidity)
}

type Records []Record

func (d reqHandler) postData(w http.ResponseWriter, r *http.Request) {
	sensor := strings.ToLower(r.PathValue("sensor"))
	if len(sensor) != 12 {
		// We only accept MAC address, which should have 12 hex digits.
		http.Error(w, "bad tag: should have 12 hex digits", http.StatusBadRequest)
		return
	}
	if strings.ContainsFunc(sensor, func(r rune) bool {
		switch {
		case r >= '0' && r <= '9':
			return false
		case r >= 'a' && r <= 'f':
			return false
		}
		return true
	}) {
		http.Error(w, "bad tag: contains not a hex digit", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var rd Record
	if err = json.Unmarshal(body, &rd); err != nil {
		http.Error(w, fmt.Sprintf("error parsing the body: %v", err), http.StatusBadRequest)
		return
	}

	if rd.Time.IsZero() {
		rd.Time = time.Now()
	}

	if err := d.store.StoreRecord(sensor, rd); err != nil {
		http.Error(w, fmt.Sprintf("error storing sensor %q: %v", err), http.StatusPreconditionFailed)
		return
	}
	log.Printf("data posted for sensor %q: %s", sensor, rd)
}

func handleFS(w http.ResponseWriter, fn string, fsys fs.FS, what string) {
	content, err := fs.ReadFile(fsys, fn)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read %s file %q: %v", what, fn, err), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	port := flag.Int("port", 8080, "Port to listen")
	dataPath := flag.String("data-path", path.Join(os.Getenv("HOME"), "pi-out"), "Path to data files")
	staticPath := flag.String("static-path", path.Join(os.Getenv("PWD"), "static"), "Path to static files")
	user := flag.String("user", "user", "Username")
	pass := flag.String("password", "password", "Password")
	flag.Parse()

	store, err := newDataStorage(*dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	d, err := NewReqHandler(store, *staticPath, *user, *pass)
	if err != nil {
		log.Fatal(err)
	}

	m := d.ServeMux()
	s := &http.Server{
		Addr:           fmt.Sprintf("0.0.0.0:%d", *port),
		Handler:        m,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Fatal(s.ListenAndServe())
}
