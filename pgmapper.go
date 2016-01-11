package pgmapper

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/InteractiveLecture/jsonpatch"
	"github.com/InteractiveLecture/pgmapper/pgutil"
	_ "github.com/lib/pq"
)

type Mapper struct {
	db *sql.DB
}

type Config struct {
	User     string
	Port     int
	Host     string
	Password string
	Ssl      bool
	Database string
}

func DefaultConfig() Config {
	return Config{
		User:     "postgres",
		Port:     5432,
		Host:     "localhost",
		Password: "",
		Ssl:      false,
		Database: "test",
	}
}

func (c Config) toString() string {
	connectionString := fmt.Sprintf("user=%s", c.User)
	if c.Database != "" {
		connectionString = fmt.Sprintf("%s dbname=%s", connectionString, c.Database)
	}
	if c.Password != "" {
		connectionString = fmt.Sprintf("%s password=%s", connectionString, c.Password)
	}
	connectionString = fmt.Sprintf("%s host=%s", connectionString, c.Host)
	connectionString = fmt.Sprintf("%s port=%d", connectionString, c.Port)
	if !c.Ssl {
		connectionString = fmt.Sprintf("%s sslmode=disable", connectionString)
	}
	return connectionString
}

func New(config Config) (*Mapper, error) {
	db, err := sql.Open("postgres", config.toString())
	if err != nil {
		return nil, err
	}
	return &Mapper{db}, nil
}

func (mapper *Mapper) ApplyPatch(patch *jsonpatch.Patch, compiler jsonpatch.PatchCompiler, options map[string]interface{}) error {
	options["db"] = mapper.db
	commands, err := compiler.Compile(patch, options)
	if err != nil {
		return err
	}
	results := make([]interface{}, 0)
	tx, err := mapper.db.Begin()
	if err != nil {
		return err
	}
	log.Println("starting patch-transaction...")
	for _, com := range commands.Commands {
		res, err := com.ExecuteBefore(tx)
		if err != nil {
			tx.Rollback()
			return err
		}
		results = append(results, res)
	}
	for i, com := range commands.Commands {
		results[i], err = com.ExecuteMain(tx, results[i])
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	for i, com := range commands.Commands {
		results[i], err = com.ExecuteAfter(tx, results[i])
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (t *Mapper) QueryIntoBytes(query string, params ...interface{}) ([]byte, error) {
	row, err := t.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer row.Close()
	var result = make([]byte, 0)
	for row.Next() {
		var tmp = make([]byte, 0)
		err = row.Scan(&tmp)
		if err != nil {
			return nil, err
		}
		result = append(result, tmp...)
	}
	return result, nil
}

func (t *Mapper) PreparedQueryIntoBytes(query string, params ...interface{}) ([]byte, error) {
	stmt, parsedParams := pgutil.Prepare(query, params...)
	return t.QueryIntoBytes(stmt, parsedParams...)
}

func (t *Mapper) Execute(query string, params ...interface{}) error {
	stmt, parsedParams := pgutil.Prepare(query, params...)
	_, err := t.db.Exec(stmt, parsedParams...)
	return err
}
func (t *Mapper) ExecuteRaw(query string, params ...interface{}) (sql.Result, error) {
	return t.db.Exec(query, params...)
}
