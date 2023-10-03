package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/themoment198/ssqlquery"
	"log"
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
}

func init() {
	db, err := sql.Open("mysql", "debian-sys-maint:KwDeN6L0dKsxdXXR@tcp(localhost:3306)/test?multiStatements=true&allowNativePasswords=true&clientFoundRows=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	initSql := `
drop database if exists d1;
create database d1;
use d1;

drop table if exists t1;
create table t1(c1 int not null AUTO_INCREMENT PRIMARY KEY, c2 float not null default 0, c3 double not null default 0, c4 varchar(16) not null default '', c5 dec(10,2) not null default 0.0, c6 datetime not null default '1990-01-01', c7 blob not null);

insert into t1 values(1, 2.2, 3.3, 'hello world', 23.43, '1988-09-09', 'hello world');
insert into t1 values(2, 4.4, 5.5, 'foo bar', 45.65, '1988-10-09', 'foo bar');
insert into t1(c7) values('...');
`
	_, err = db.Exec(initSql)
	if err != nil {
		panic(err)
	}
}

func TestQuery() {
	db, err := sql.Open("mysql", "debian-sys-maint:KwDeN6L0dKsxdXXR@tcp(localhost:3306)/d1?allowNativePasswords=true&clientFoundRows=true")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	type om struct {
		C7 string `sql:"c7"`
	}
	result := new([]om)
	err = ssqlquery.Query(db, result, "select * from t1 where c1 = 1")
	if err != nil {
		log.Fatal(err)
	}

	log.Print(*result)

	result1 := new([]om)
	err = ssqlquery.Query(db, result1, "select * from t1 where c1 = 2")
	if err != nil {
		log.Fatal(err)
	}

	log.Print(*result1)
}

func TestQueryByTX() {
	db, err := sql.Open("mysql", "debian-sys-maint:KwDeN6L0dKsxdXXR@tcp(localhost:3306)/d1?allowNativePasswords=true&clientFoundRows=true")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	type om1 struct {
		C1 int `sql:"c1"`
	}
	result := new([]om1)
	err = ssqlquery.Query(tx, result, "select * from t1")
	if err != nil {
		{
			err := tx.Rollback()
			if err != nil {
				log.Print(err)
			}
		}
		log.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	log.Print(*result)
}

func main() {
	TestQuery()
	TestQueryByTX()
}
