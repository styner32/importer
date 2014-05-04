package importer

import (
	"database/sql"
	"fmt"
	"github.com/jmcvetta/neoism"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v1"
	"log"
	"os"
	"reflect"
)

type Config struct {
	Source struct {
		User   string
		Dbname string
	}
	Target struct {
		Url    string
		UrlUrl string
	}
	Mappings []struct {
		Fromitem         string
		Toitem           string
		Intermediateitem string
		Relationname     string
	}
}

type Identifier struct {
	Id   int64
	Code string
}

const (
	DEFAULT_BATCH_SIZE int = 1024
)

func Run() {
	db_config := GetDbConfig("config.yml")
	config_to_str := fmt.Sprintf("user=%s dbname=%s sslmode=disable", db_config.Source.User, db_config.Source.Dbname)
	fmt.Printf("Config: %s\n", config_to_str)
	source_db, err := sql.Open("postgres", config_to_str)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Postgres Db: %v\n", source_db)
	target_db, err := neoism.Connect(db_config.Target.Url)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Neo4j Db: %v\n", target_db)

	ClearDatabase(target_db)
	ResetIndexes(target_db, db_config)

	for _, mapping := range db_config.Mappings {
		ImportRelationship(source_db, target_db, mapping.Fromitem, mapping.Toitem, mapping.Intermediateitem, mapping.Relationname)
	}
}

func GetDbConfig(configFilename string) *Config {
	file, err := os.Open(configFilename)
	if err != nil {
		log.Fatal(err)
	}

	data := make([]byte, 1024)
	count, err := file.Read(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Read: %d\n", count)
	fmt.Printf("Content: %s\n", data[0:count])

	var dbConfig Config
	err = yaml.Unmarshal(data[0:count], &dbConfig)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("--- t:\n%v\n\n", dbConfig)
	return &dbConfig
}

func ImportRelationship(sourceDb *sql.DB, targetDb *neoism.Database, fromItem string, toItem string, intermediateItem string, relationName string) {
	fromNodeName := Titleize(fromItem)
	toNodeName := Titleize(toItem)

	intermediateTableName := Pluralize(intermediateItem)

	fromIdColumn := ToIdColumn(fromItem)
	toIdColumn := ToIdColumn(toItem)
	toCodeColumn := ToCodeColumn(toItem)

	GetNumberOfRows(sourceDb, intermediateTableName)

	query := fmt.Sprintf("SELECT * FROM %s", intermediateTableName)
	rows, err := sourceDb.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}

	column2value := make(map[string]*interface{})
	valuePtrs := make([]interface{}, len(columns))

	for i, column := range columns {
		var value interface{}
		column2value[column] = &value
		valuePtrs[i] = &value
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Fatal(err)
		}

		relationProperties := make(map[string]interface{})
		var fromNode *neoism.Node
		var toNode *neoism.Node

		for i, valuePtr := range valuePtrs {
			value := (*valuePtr.(*interface{}))
			key := columns[i]
			if value != nil {
				switch key {
				default:
					if reflect.ValueOf(value).Kind() == reflect.Slice {
						relationProperties[key] = Uint8ToString(value.([]uint8))
					} else {
						relationProperties[key] = value
					}
				case fromIdColumn:
					fromNode = FindOrCreateNode(targetDb, fromNodeName, Identifier{Id: value.(int64)})
				case toIdColumn:
					toNode = FindOrCreateNode(targetDb, toNodeName, Identifier{Id: value.(int64)})
				case toCodeColumn:
					toNode = FindOrCreateNode(targetDb, toNodeName, Identifier{Code: Uint8ToString(value.([]uint8))})
				}
			}
		}

		_, err := fromNode.Relate(relationName, toNode.Id(), neoism.Props(relationProperties))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func ClearDatabase(db *neoism.Database) {
	cypher_query := neoism.CypherQuery{
		Statement: `
      MATCH (n)
      OPTIONAL MATCH (n)-[r]-()
      DELETE n,r
    `,
	}

	err := db.Cypher(&cypher_query)
	if err != nil {
		log.Fatal(err)
	}
}

func ResetIndexes(db *neoism.Database, config *Config) {
	set := make(map[string]bool)

	for i := 0; i < len(config.Mappings); i++ {
		set[Titleize(config.Mappings[i].Fromitem)] = true
		set[Titleize(config.Mappings[i].Toitem)] = true
	}

	for key, _ := range set {
		DropUniqunessContraintTo(db, key)
		CreateUniqunessContraintTo(db, key)
	}
}

func GetNumberOfRows(db *sql.DB, tableName string) {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s count: %d\n", tableName, count)
}

func DropUniqunessContraintTo(db *neoism.Database, nodeName string) {
	statement := fmt.Sprintf("DROP CONSTRAINT ON (item:%s) ASSERT item.id IS UNIQUE", nodeName)
	cypher_query := neoism.CypherQuery{
		Statement: statement,
	}

	err := db.Cypher(&cypher_query)
	if err != nil {
		fmt.Println(err)
	}
}

func CreateUniqunessContraintTo(db *neoism.Database, nodeName string) {
	statement := fmt.Sprintf("CREATE CONSTRAINT ON (item:%s) ASSERT item.id IS UNIQUE", nodeName)
	cypher_query := neoism.CypherQuery{
		Statement: statement,
	}

	err := db.Cypher(&cypher_query)
	if err != nil {
		log.Fatal(err)
	}
}

func FindOrCreateNode(db *neoism.Database, nodeName string, identifier Identifier) *neoism.Node {
	resource := []struct {
		N neoism.Node
	}{}

	parameters := neoism.Props{}
	if identifier.Id != 0 {
		parameters["id"] = identifier.Id
	} else if identifier.Code != "" {
		parameters["id"] = identifier.Code
	} else {
		log.Fatal("Wrong Identifier: %v", identifier)
	}

	statement := fmt.Sprintf("MATCH (n:%s) WHERE n.id = {id} RETURN n", nodeName)
	cypher_query := neoism.CypherQuery{
		Statement:  statement,
		Parameters: parameters,
		Result:     &resource,
	}

	err := db.Cypher(&cypher_query)
	if err != nil {
		log.Fatal(err)
	}

	statement = fmt.Sprintf("CREATE (n:%s {id: {id}}) RETURN n", nodeName)
	if len(resource) == 0 {
		cypher_query := neoism.CypherQuery{
			Statement:  statement,
			Parameters: parameters,
			Result:     &resource,
		}

		err := db.Cypher(&cypher_query)
		if err != nil {
			log.Fatal(err)
		}
	}

	node := resource[0].N
	node.Db = db
	return &node
}
