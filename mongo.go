package xk6_mongo

import (
	"context"
	"log"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	k6modules "go.k6.io/k6/js/modules"
)

// Register the extension on module initialization, available to
// import from JS as "k6/x/mongo".
func init() {
	k6modules.Register("k6/x/mongo", new(Mongo))
}

// Mongo is the k6 extension for a Mongo client.
type Mongo struct{}

// Client is the Mongo client wrapper.
type Client struct {
	client *mongo.Client
}

type Sort struct {
	Asc   bool
	Field string
}

type Options struct {
	Limit int64
	Sort  []Sort
}

// NewClient represents the Client constructor (i.e. `new mongo.Client()`) and
// returns a new Mongo client object.
// connURI -> mongodb://username:password@address:port/db?connect=direct
func (*Mongo) NewClient(connURI string) interface{} {

	clientOptions := options.Client().ApplyURI(connURI)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}

	return &Client{client: client}
}

const filter_is string = "filter is "

func (c *Client) Insert(database string, collection string, doc interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertOne(context.TODO(), doc)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) InsertMany(database string, collection string, docs []any) error {
	log.Printf("Insert multiple documents")
	db := c.client.Database(database)
	col := db.Collection(collection)
	_, err := col.InsertMany(context.TODO(), docs)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Find(database string, collection string, filter interface{}) []bson.M {
	db := c.client.Database(database)
	col := db.Collection(collection)
	log.Print(filter_is, filter)
	cur, err := col.Find(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
	}
	var results []bson.M
	if err = cur.All(context.TODO(), &results); err != nil {
		panic(err)
	}
	return results
}

func (c *Client) FindWithLimit(database string, collection string, filter interface{}, opts interface{}, fields interface{}) []bson.M {
	start := time.Now()
	db := c.client.Database(database)
	col := db.Collection(collection)
	log.Print(filter_is, filter)
	log.Print("opts ", opts)
	log.Print("Fields are ", fields)

	var optsStruct Options
	config := &mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      &optsStruct,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	if err := decoder.Decode(opts); err != nil {
		log.Fatal(err)
		return nil
	}

	log.Print("opts are ", optsStruct)

	options := options.Find()

	// Setting up limits if filled
	if optsStruct.Limit != 0 {
		options.SetLimit(optsStruct.Limit)
	}

	// Setting up sort order is filled
	if len(optsStruct.Sort) != 0 {
		sortStruc := bson.D{}
		for _, sort := range optsStruct.Sort {
			if sort.Asc {
				sortStruc = append(sortStruc, bson.E{sort.Field, 1})
			} else {
				sortStruc = append(sortStruc, bson.E{sort.Field, -1})
			}
		}
		options.SetSort(sortStruc)
	}

	options.SetProjection(fields)

	marchaled_filter, err := bson.Marshal(filter)
	if err != nil {
		log.Fatal(err)
	}

	cur, err := col.Find(context.TODO(), marchaled_filter, options)
	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	// log.Print("FindWithLimit getting cursor took ", elapsed, " for filter ", filter)

	var list = make([]bson.M, 0)

	defer cur.Close(context.TODO())
	for cur.Next(context.TODO()) {
		// t_inner := time.Now()
		var result bson.M
		err = cur.Decode(&result)

		if err != nil {
			log.Println(err)
			return nil
		}

		list = append(list, result)
		// elapsed_inner := time.Now()
		// elapsed_inner_e := elapsed_inner.Sub(t_inner)
		// log.Print("FindWithLimit reading cursor took ", elapsed_inner_e, " for filter ", filter)

	}
	if err := cur.Err(); err != nil {
		return nil
	}

	t = time.Now()
	elapsed = t.Sub(start)
	log.Print("FindWithLimit took ", elapsed, " for filter ", filter)
	return list
}

func (c *Client) FindOne(database string, collection string, filter interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	var result bson.M
	opts := options.FindOne().SetSort(bson.D{{"_id", 1}})
	log.Print(filter_is, filter)
	err := col.FindOne(context.TODO(), filter, opts).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found document %v", result)
	return nil
}

func (c *Client) FindAll(database string, collection string) []bson.M {
	log.Printf("Find all documents")
	db := c.client.Database(database)
	col := db.Collection(collection)
	cur, err := col.Find(context.TODO(), bson.D{{}})
	if err != nil {
		log.Fatal(err)
	}
	var results []bson.M
	if err = cur.All(context.TODO(), &results); err != nil {
		panic(err)
	}
	return results
}

func (c *Client) DeleteOne(database string, collection string, filter interface{}) error {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Delete().SetHint(bson.D{{"_id", 1}})
	log.Print(filter_is, filter)
	result, err := col.DeleteOne(context.TODO(), filter, opts)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Deleted documents %v", result)
	return nil
}

func (c *Client) DeleteMany(database string, collection string, filter interface{}) (int64, error) {
	db := c.client.Database(database)
	col := db.Collection(collection)
	opts := options.Delete().SetHint(bson.D{{"_id", 1}})
	log.Print(filter_is, filter)
	result, err := col.DeleteMany(context.TODO(), filter, opts)
	if err != nil {
		log.Fatal(err)
		return -1, err
	}
	log.Printf("Deleted documents %v", result)
	return result.DeletedCount, nil
}

func (c *Client) DropCollection(database string, collection string) error {
	log.Printf("Delete collection if present")
	db := c.client.Database(database)
	col := db.Collection(collection)
	err := col.Drop(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func (c *Client) UpdateMany(database string, collection string, filter interface{}, update interface{}) (int64, int64) {
	log.Printf("Updating collection if present")
	db := c.client.Database(database)
	col := db.Collection(collection)

	updateResult, err := col.UpdateMany(context.TODO(), filter, update)

	if err != nil {
		log.Fatal(err)
	}
	return updateResult.MatchedCount, updateResult.ModifiedCount
}

func (c *Client) Diconnect() {
	log.Printf("Dicsonnecting from Mongo database")
	defer func() {
		if err := c.client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()
}
