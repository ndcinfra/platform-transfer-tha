package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/joho/godotenv"
	"golang.org/x/crypto/scrypt"
)

type THUser struct {
	UId         string `json:"u_id"`
	Email       string `json:"email"`
	Displayname string `json:"displayname"`
}

type PreparedNAUser struct {
	u_id         int64
	display_name string
	email        string
	password     string
	salt         string
	transfer_id  string
}

const pwHashBytes = 64

// it not exists, insert into user table
//		-- transfer_no = 0
//		-- transfer_id = tha's uid (string)
//		-- displayname = 6자리 랜덤문자_displayname
//		-- provider = tha
//		-- country = na
//		-- Confirmed = true
var insertSql = "INSERT INTO public.user (" +
	"u_id, display_name, email, password, salt, confirmed, birth, provider, " +
	"transfer_user_no, transfer_user_id, permission, status, ip, country, " +
	"create_at, update_at, confirmed_promotion)" +
	" VALUES (" +
	"$1, $2, $3, $4, $5, true, '01/01/1900', 'tha'," +
	"0, $6, 'user', 'normal', '1.1.1.1', 'na'," +
	"current_timestamp, current_timestamp, false );"

/**
 * mode: qa or prod
 * transfer: account ot wallet
 *
 * go run main.go -mode=qa -transfer=account
 *
 */

func main() {
	mode := flag.String("mode", "qa", "qa or prod")
	transfer := flag.String("transfer", "account", "account ot wallet")

	required := []string{"mode", "transfer"}
	flag.Parse()
	seen := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, req := range required {
		if !seen[req] {
			fmt.Fprintf(os.Stderr, "missing required -%s argument/flag\n", req)
			os.Exit(1)
		}
	}

	fmt.Printf("mode: %s, transfer: %s\n", *mode, *transfer)

	start := time.Now()

	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file")
	}

	thhost := strings.ToUpper(*mode) + "_TH_DBHOST"
	nahost := strings.ToUpper(*mode) + "_NA_DBHOST"

	TH_DB_CON := os.Getenv(thhost)
	NA_DB_CON := os.Getenv(nahost)

	th_dbconn, err := pgx.Connect(context.Background(), TH_DB_CON)
	if err != nil {
		fmt.Printf("tha db conn error: %s\n", err)
		os.Exit(1)
	}

	na_dbconn, err := pgx.Connect(context.Background(), NA_DB_CON)
	if err != nil {
		fmt.Printf("na db conn error: %s\n", err)
		os.Exit(1)
	}

	if *transfer == "account" {
		runAccountTransfer(th_dbconn, na_dbconn)
	} else if *transfer == "wallet" {
		//
	} else {
		fmt.Println("transfer input error")
		os.Exit(1)
	}

	//
	fmt.Println("Done. elasped time: ", time.Since(start))
	os.Exit(0)
}

func runAccountTransfer(th_dbconn, na_dbconn *pgx.Conn) {
	// get the list from tha by orderby createdAt
	// count: 70193
	sql := "SELECT \"UID\" as u_id, email, displayname FROM \"user\" where confirmed = true order by create_at asc"
	thRows, err := th_dbconn.Query(context.Background(), sql)
	if err != nil {
		fmt.Printf("conn.Query failed: %v\n", err)
		os.Exit(1)
	}
	defer thRows.Close()

	// loop
	// var thaUsers []User
	/*

		var batchRowCount int
		batch := &pgx.Batch{}
		var preparedNAU PreparedNAUser
	*/
	totlaRowCount := 0

	for thRows.Next() {
		var thu THUser

		thRows.Scan(
			&thu.UId, &thu.Email, &thu.Displayname,
		)

		//thaUsers = append(thaUsers, u)
		if isExistEmail(thu, na_dbconn) {

			fmt.Printf("exists the mail: %s\n", thu.Email)
			// insert conflict table with go routine
			// if already exists, insert confflit user table
			insertConflict(thu, na_dbconn)

		} else {
			// fmt.Printf("%+v\n", thu)
			// make password
			// make salt
			salt, err := generateSalt()
			if err != nil {
				fmt.Println("error make salt: ", err)
				os.Exit(1)
			}
			hash, err := generatePassHash(generateRandStringLong(), salt)
			if err != nil {
				fmt.Println("error hash salt: ", err)
				os.Exit(1)
			}
			_, err = na_dbconn.Exec(
				context.Background(),
				insertSql,
				generateBigintID(),
				generateRandString()+"_"+thu.Displayname,
				strings.ToLower(thu.Email),
				hash,
				salt,
				thu.UId,
			)

			if err != nil {
				fmt.Println("error insert: ", err)
				os.Exit(1)
			}

			// not exists.
			// make bulk insert
			//var preparedNAU PreparedNAUser
			/*
				preparedNAUserInfo(&preparedNAU, thu)

				fmt.Printf("%+v\n", preparedNAU)

				// make bulk insert queue
				batch.Queue(
					insertSql,
					preparedNAU.u_id,
					preparedNAU.display_name,
					preparedNAU.email,
					preparedNAU.password,
					preparedNAU.salt,
					preparedNAU.transfer_id,
				)
				batchRowCount++
			*/
		}

		totlaRowCount++
		fmt.Printf("count: %d, email: %s\n", totlaRowCount, thu.Email)
	}

	//for test
	//u := User{UId: "xxxx", Email: "youngtip@gmail.com", Displayname: "xxx"}
	//checkExistEmail(u, na_dbconn)

	fmt.Printf("totlaRowCount: %d \n", totlaRowCount)

	// bulk inserts
	/*
		br := na_dbconn.SendBatch(context.Background(), batch)
		for i := 0; i < batchRowCount; i++ {
			ct, err := br.Exec()
			if err != nil {
				fmt.Println("bulk insert error:  ", i, err)
			}

			fmt.Println("count: ", i, "result: ", ct.RowsAffected())
		}
	*/

	// make a backup file with whole THA user list
}

func isExistEmail(thaUser THUser, na_dbconn *pgx.Conn) bool {
	//check already exists toLower(email) both tha and na
	// if already exists, insert confflit user table
	// ex. where TRIM(BOTH FROM lower(email))= TRIM(BOTH FROM lower(' youngtip@gmail.com'));
	// fmt.Println(thaUser.Email)

	sql := "select u_id from public.user where TRIM(BOTH FROM lower(email))= TRIM(BOTH FROM lower('" + thaUser.Email + "'))"
	rows, err := na_dbconn.Query(context.Background(), sql)
	// if not exists, err is exists "no rows in result set"
	// if exists,

	if err != nil {
		fmt.Printf("conn.Query failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
	}

	if rowCount > 0 {

		return true

	}
	return false
}

func preparedNAUserInfo(nau *PreparedNAUser, thaUser THUser) {

	// make u_id
	nau.u_id = generateBigintID()

	// make display_name
	nau.display_name = generateRandString() + "_" + thaUser.Displayname

	// set email
	nau.email = strings.ToLower(thaUser.Email)

	// make password
	// make salt
	salt, err := generateSalt()
	if err != nil {
		fmt.Println("error make salt: ", err)
		os.Exit(1)
	}
	hash, err := generatePassHash(generateRandStringLong(), salt)
	if err != nil {
		fmt.Println("error hash salt: ", err)
		os.Exit(1)
	}

	// set password & salt
	nau.password = hash
	nau.salt = salt

	// set transfer_user_id
	nau.transfer_id = thaUser.UId
}

func insertConflict(thaUser THUser, na_dbconn *pgx.Conn) {
	sql := "INSERT INTO public.user_conflict(u_id, display_name, email) VALUES ($1, $2, $3);"
	_, err := na_dbconn.Exec(context.Background(), sql, thaUser.UId, thaUser.Displayname, thaUser.Email)
	if err != nil {
		log.Println("error insert user_conflict: ", err)
	} else {
		log.Printf("success insert user_conflict: %v\n", thaUser)
	}

}

func generateRandString() string {
	b := make([]byte, 4) //equals 6 charachters
	rand.Read(b)
	s := hex.EncodeToString(b)
	s = strings.ToUpper(s)
	return s
}

func generateRandStringLong() string {
	b := make([]byte, 14) //equals 16 charachters
	rand.Read(b)
	s := hex.EncodeToString(b)
	s = strings.ToUpper(s)
	return s
}

func generateBigintID() int64 {
	return time.Now().UTC().UnixNano()
}

func generateSalt() (salt string, err error) {
	buf := make([]byte, pwHashBytes)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", buf), nil
}

func generatePassHash(password string, salt string) (hash string, err error) {
	h, err := scrypt.Key([]byte(password), []byte(salt), 16384, 8, 1, pwHashBytes)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h), nil
}
