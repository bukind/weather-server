package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"html/template"
	"io"
	"io/fs"
	"log"
	"maps"
	"math"
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
	db         *sql.DB
	sensorsStm *sql.Stmt
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

	tableSpec := `(
		sensor_id INTEGER,
		timestamp DATETIME,
		temperature FLOAT,
		pressure FLOAT,
		humidity FLOAT,
		cnt INTEGER DEFAULT 1,
		temperature_sigma2 FLOAT DEFAULT 0,
		pressure_sigma2 FLOAT DEFAULT 0,
		humidity_sigma2 FLOAT DEFAULT 0,
		squeezed_by INTEGER DEFAULT 1
	)`

	createData := `CREATE TABLE IF NOT EXISTS measurements ` + tableSpec + `;`
	if _, err := db.Exec(createData); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create measurements table: %v", err)
	}
	createData = `CREATE TEMP TABLE IF NOT EXISTS squeezed ` + tableSpec + `;`
	if _, err := db.Exec(createData); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create measurements table: %v", err)
	}

	sensorsStm, err := db.Prepare(`SELECT id, name FROM sensors;`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare sensors statement: %v", err)
	}

	return &dataStorage{
		db:         db,
		sensorsStm: sensorsStm,
	}, nil
}

func (d *dataStorage) Close() {
	d.sensorsStm.Close()
	d.db.Close()
}

// Return the list of available sensors.
func (d *dataStorage) Sensors() (map[string]int64, error) {
	// TODO: cache
	rows, err := d.sensorsStm.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		result[name] = id
	}
	return result, nil
}

func (d *dataStorage) ReadData(ctx context.Context, sensor string, startDate, endDate time.Time) (Data, error) {
	sensors, err := d.Sensors()
	if err != nil {
		return Data{}, err
	}
	sensorID, ok := sensors[sensor]
	if !ok {
		return Data{}, fmt.Errorf("sensor %q is not found", sensor)
	}
	// TODO: replace with prepared statement.
	query := `SELECT
	timestamp, temperature, pressure, humidity
	FROM measurements
	WHERE sensor_id = ? AND timestamp >= ? AND timestamp <= ?
	ORDER BY timestamp;`
	rows, err := d.db.QueryContext(ctx, query, sensorID, startDate, endDate)
	if err != nil {
		return Data{}, fmt.Errorf("failed to read data for sensor %q: %v", sensor, err)
	}
	return Data{
		rows: rows,
	}, nil
}

func (d *dataStorage) StoreRecord(ctx context.Context, sensor string, rd Record) error {
	sensors, err := d.Sensors()
	if err != nil {
		return err
	}
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
			sensors, err = d.Sensors()
			if err != nil {
				return err
			}
			sensorID, ok = sensors[sensor]
			if !ok {
				return fmt.Errorf("sensor %q is not found after insert", sensor)
			}
		}
	}
	_, err = d.db.Exec(`INSERT INTO measurements (sensor_id, timestamp, temperature, pressure, humidity)
	VALUES (?,?,?,?,?);`, sensorID, rd.Time, rd.Temperature, rd.Pressure, rd.Humidity)
	return err
}

// Remove all rows with the same squeezed_by from squeezed.
func (s squeezer) removeSqueezedBy() error {
	log.Printf("removing previously squeezed rows from squeezed")
	query := `DELETE FROM squeezed WHERE squeezed_by = ? AND timestamp >= ? AND timestamp < ?;`
	res, err := s.tx.Exec(query, s.secondsToSqueeze, s.startTime, s.endTime)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	log.Printf("%d rows removed", rows)
	return nil
}

func (s squeezer) readSqueezedData() (Data, error) {
	log.Printf("reading squeeze data in the interval [%s,%s]", s.startTime, s.endTime)
	query := `SELECT
	sensor_id,
	STRFTIME("%s", timestamp)/?*?+? AS seconds_mean,
	SUM(cnt) AS row_count,
	SUM(temperature*cnt) AS temperature_sum,
	SUM((temperature*temperature + temperature_sigma2)*cnt) AS temperature_sum2,
	SUM(pressure*cnt) AS pressure_sum,
	SUM((pressure*pressure + pressure_sigma2)*cnt) AS pressure_sum2,
	SUM(humidity*cnt) AS humidity_sum,
	SUM((humidity*humidity + humidity_sigma2)*cnt) AS humidity_sum2
	FROM measurements
	WHERE squeezed_by < ? AND timestamp >= ? AND timestamp < ?
	GROUP BY sensor_id, seconds_mean
	ORDER BY sensor_id, seconds_mean;
	`
	rows, err := s.tx.Query(query, s.secondsToSqueeze, s.secondsToSqueeze, s.secondsToSqueeze/2, s.secondsToSqueeze, s.startTime, s.endTime)
	if err != nil {
		return Data{}, fmt.Errorf("failed to read squeezed data: %v", err)
	}
	return Data{
		rows: rows,
	}, nil
}

func (s squeezer) insertSqueezed(data Data) (bool, error) {
	defer data.Close()
	log.Printf("inserting squeeze data")
	stm, err := s.tx.Prepare(`INSERT INTO squeezed (
		sensor_id,
		timestamp,
		temperature,
		pressure,
		humidity,
		cnt,
		temperature_sigma2,
		pressure_sigma2,
		humidity_sigma2,
		squeezed_by
	) VALUES (
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?
	);`)
	if err != nil {
		return false, err
	}
	defer stm.Close()
	rows := 0
	for ; ; rows++ {
		var sensorID int
		var seconds int64
		var count int
		var tempSum float64
		var temp2Sum float64
		var pressSum float64
		var press2Sum float64
		var humSum float64
		var hum2Sum float64
		ok, err := data.Read(&sensorID, &seconds, &count, &tempSum, &temp2Sum, &pressSum, &press2Sum, &humSum, &hum2Sum)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		var tempMean, tempSig2, pressMean, pressSig2, humMean, humSig2 float64
		if count > 0 {
			tempMean = tempSum / float64(count)
			tempSig2 = math.Max(temp2Sum/float64(count)-tempMean*tempMean, 0.)
			pressMean = pressSum / float64(count)
			pressSig2 = math.Max(press2Sum/float64(count)-pressMean*pressMean, 0.)
			humMean = humSum / float64(count)
			humSig2 = math.Max(hum2Sum/float64(count)-humMean*humMean, 0.)
		}
		if _, err := stm.Exec(sensorID, time.Unix(seconds, 0), tempMean, pressMean, humMean, count, tempSig2, pressSig2, humSig2, s.secondsToSqueeze); err != nil {
			return false, err
		}
	}
	log.Printf("inserted %d rows", rows)
	return rows > 0, nil
}

func (s squeezer) removeSubSqueezed() error {
	log.Printf("removing squeezed rows from measurements")
	query := `DELETE FROM measurements WHERE squeezed_by < ? AND timestamp >= ? AND timestamp < ?;`
	res, err := s.tx.Exec(query, s.secondsToSqueeze, s.startTime, s.endTime)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	log.Printf("%d rows removed", rows)
	return nil
}

func (s squeezer) moveSqueezedData() error {
	log.Printf("moving squeezed rows from squeezed into measurements")
	query := `INSERT INTO measurements
	SELECT * FROM squeezed
	WHERE squeezed_by = ? AND timestamp >= ? AND timestamp < ?;
	`
	res, err := s.tx.Exec(query, s.secondsToSqueeze, s.startTime, s.endTime)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	log.Printf("%d rows inserted", rows)
	return nil
}

func (s squeezer) Exec() (err error) {
	start := time.Now()
	defer func() {
		log.Printf("squeeze completed in %s", time.Now().Sub(start))
	}()
	defer func() {
		if err != nil {
			err = errors.Join(err, s.tx.Rollback())
		} else {
			err = s.tx.Commit()
		}
	}()

	// Remove previously squeezed data, if any.
	if err := s.removeSqueezedBy(); err != nil {
		return err
	}
	// Read data from measurements.
	data, err := s.readSqueezedData()
	if err != nil {
		return err
	}
	// Insert rows into squeezed.
	if ok, err := s.insertSqueezed(data); err != nil || !ok {
		return err
	}
	if err := s.removeSubSqueezed(); err != nil {
		return err
	}
	// Move rows from squeezed to measurements.
	return s.moveSqueezedData()
}

type squeezer struct {
	tx               *sql.Tx
	secondsToSqueeze int64
	startTime        time.Time
	endTime          time.Time
}

func (d *dataStorage) Squeeze(interval time.Duration, startTime, endTime time.Time) error {
	s, err := d.newSqueezer(interval, startTime, endTime)
	if err != nil {
		return err
	}
	return s.Exec()
}

func (d *dataStorage) newSqueezer(interval time.Duration, startTime, endTime time.Time) (squeezer, error) {
	if interval <= time.Second {
		return squeezer{}, fmt.Errorf("interval %s is too small for squeeze", interval)
	}
	if interval/(2*time.Second)*(2*time.Second) != interval {
		return squeezer{}, fmt.Errorf("interval %s is not multiple of a (two seconds)", interval)
	}
	if time.Hour/interval*interval != time.Hour {
		return squeezer{}, fmt.Errorf("interval %s is not a factor or an hour", interval)
	}
	secondsToSqueeze := int64(interval / time.Second)
	endTime = endTime.Truncate(time.Hour)
	startTime = startTime.Truncate(time.Hour)

	tx, err := d.db.Begin()
	if err != nil {
		return squeezer{}, err
	}
	return squeezer{
		tx:               tx,
		secondsToSqueeze: secondsToSqueeze,
		startTime:        startTime,
		endTime:          endTime,
	}, nil
}

func (d *dataStorage) squeezeAll() error {
	// The interval [-3d, -1d] -> 10 second.
	// The interval [-7d, -3d] -> 60 second.
	// The interval [-30d, -7d] -> 600 second.
	// The interval [-oo, -30d] -> 3600 second.
	endTime1 := time.Now().Add(-time.Hour * 24).Truncate(time.Hour)
	endTime2 := endTime1.Add(-time.Hour * 24 * 2)
	endTime3 := endTime2.Add(-time.Hour * 24 * 4)
	endTime4 := endTime3.Add(-time.Hour * 24 * 23)
	endTime5 := endTime4.Add(-time.Hour * 24 * 3560)
	for _, sq := range []struct {
		interval  time.Duration
		startTime time.Time
		endTime   time.Time
	}{
		{time.Second * 10, endTime2, endTime1},
		{time.Second * 60, endTime3, endTime2},
		{time.Second * 600, endTime4, endTime3},
		{time.Second * 3600, endTime5, endTime4},
	} {
		if err := d.Squeeze(sq.interval, sq.startTime, sq.endTime); err != nil {
			return err
		}
	}
	return nil
}

type Data struct {
	rows *sql.Rows
}

// Close must be called on Data at the end of reading.
func (d Data) Close() {
	if d.rows != nil {
		d.rows.Close()
	}
}

// Read prepares the next row and reads it into the passed variables.
// It returns:
//   - (true, nil) on success,
//   - (false, nil) on no data,
//   - (false, err) on failure.
func (d Data) Read(into ...any) (bool, error) {
	if d.rows == nil {
		return false, nil
	}
	ok := d.rows.Next()
	if !ok {
		return ok, d.rows.Err()
	}
	if err := d.rows.Scan(into...); err != nil {
		d.rows.Close()
		return false, err
	}
	return true, nil
}

func (d Data) Write(w http.ResponseWriter) error {
	buf := &bytes.Buffer{}
	buf.WriteString("#time,temperature,pressure,humidity\n")
	count := 0
	for {
		var rd Record
		ok, err := d.Read(&rd.Time, &rd.Temperature, &rd.Pressure, &rd.Humidity)
		if err != nil {
			httpError(w, fmt.Sprintf("failed to read: %v", err), http.StatusPreconditionFailed)
			return err
		}
		if !ok {
			if count == 0 {
				httpError(w, "no data found in the range", http.StatusNotFound)
				return nil
			}
			break
		}
		count++
		if _, err := buf.WriteString(rd.String() + "\n"); err != nil {
			httpError(w, fmt.Sprintf("failed to write buffer: %v", err), http.StatusPreconditionFailed)
			return err
		}
	}
	log.Printf("written %d rows", count)
	w.Header().Set("content-type", "text/csv")
	w.WriteHeader(http.StatusOK)
	w.Write(buf.Bytes())
	return nil
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
	m.HandleFunc("POST /upload/{sensor}", basicAuth(d.uploadSensorData, d.username, d.password))
	return m
}

func (d reqHandler) handleIndex(w http.ResponseWriter, r *http.Request) {
	log.Printf("handling index")
	sensors, err := d.store.Sensors()
	if err != nil {
		httpError(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	w.WriteHeader(http.StatusOK)
	indexTemplate.Execute(w, map[string]any{
		"Sensors": slices.Collect(maps.Keys(sensors)),
	})
}

func (d reqHandler) handleStatic(w http.ResponseWriter, r *http.Request) {
	fn := r.PathValue("objectname")
	log.Printf("handling static %q", fn)
	handleFS(r.Context(), w, fn, d.staticFS, "static")
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
	sensors, err := d.store.Sensors()
	if err != nil {
		httpError(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	sensor := r.PathValue("sensor")
	log.Printf("sensor %s, start %s, end %s", sensor, startDate, endDate)
	if _, ok := sensors[sensor]; !ok {
		httpError(w, fmt.Sprintf("unknown sensor %q", sensor), http.StatusBadRequest)
		return
	}
	data, err := d.store.ReadData(r.Context(), sensor, startDate, endDate)
	if err != nil {
		httpError(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	defer data.Close()
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

func (d reqHandler) uploadSensorData(w http.ResponseWriter, r *http.Request) {
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

	if err := d.store.StoreRecord(r.Context(), sensor, rd); err != nil {
		http.Error(w, fmt.Sprintf("error storing sensor %q: %v", err), http.StatusPreconditionFailed)
		return
	}
	log.Printf("data posted for sensor %q: %s", sensor, rd)
}

func handleFS(ctx context.Context, w http.ResponseWriter, fn string, fsys fs.FS, what string) {
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
	createOnly := flag.Bool("create-only", false, "Only create the database")

	flag.Parse()

	store, err := newDataStorage(*dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	if *createOnly {
		return
	}

	if err := store.squeezeAll(); err != nil {
		log.Fatal(err)
	}

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
