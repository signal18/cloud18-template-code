// Cloud18 sample Go application: says hello and proves the database path end to end.
// The template passes the connection through the environment (DB_HOST is the cluster's
// proxy, so the answer comes from the current primary and follows a switchover).
package main

import (
	"database/sql"
	"fmt"
	"html"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	host, port, name := env("DB_HOST", "127.0.0.1"), env("DB_PORT", "3306"), env("DB_NAME", "")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=3s", env("DB_USER", ""), env("DB_PASSWORD", ""), host, port, name)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(time.Minute)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var id int64
		var dbhost, version string
		title, detail := "", ""
		err := db.QueryRow("SELECT @@server_id, @@hostname, VERSION()").Scan(&id, &dbhost, &version)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			title, detail = "Hello world, I'm not connected to the database", err.Error()
		} else {
			title = fmt.Sprintf("Hello world, I'm connected to database server id %d", id)
			detail = fmt.Sprintf("Go, %s, %s, schema %s, through %s:%s", dbhost, version, name, host, port)
		}
		fmt.Fprintf(w, "<!DOCTYPE html>\n<html>\n  <head>\n    <meta charset=\"utf-8\">\n    <title>Cloud18 sample Go application</title>\n  </head>\n  <body>\n    <h1>%s</h1>\n    <p>%s</p>\n  </body>\n</html>\n",
			html.EscapeString(title), html.EscapeString(detail))
	})
	if err := http.ListenAndServe(":"+env("PORT", "8080"), nil); err != nil {
		panic(err)
	}
}
