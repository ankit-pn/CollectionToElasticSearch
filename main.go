package main

import (
	"context"
	"fmt"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		panic("Error loading .env file")
	}

	// Get MongoDB URI and Elasticsearch port from the environment variables
	mongoDBURI := os.Getenv("MONGODB_URI")
	elasticSearchPort := os.Getenv("ELASTICSEARCH_PORT")

	// Connect to MongoDB
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoDBURI))
	if err != nil {
		panic(err)
	}

	// Ping MongoDB to verify connection
	err = mongoClient.Ping(context.TODO(), nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to MongoDB!")

	// Connect to Elasticsearch
	elasticSearchAddr := fmt.Sprintf("http://localhost:%s", elasticSearchPort)
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticSearchAddr},
	})
	if err != nil {
		panic(err)
	}

	// Ping Elasticsearch to verify connection
	_, err = esClient.Info()
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to Elasticsearch!")

	// Define the MongoDB collection to index
	collection := mongoClient.Database("tesseract").Collection("images")

	// Here goes the logic for indexing the documents...
	// (Omitted for brevity - please implement the logic to fetch from MongoDB and index to Elasticsearch)

	// Retrieve documents from the MongoDB collection
	cursor, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		panic(err)
	}
	defer cursor.Close(context.Background())

	// Iterate through the cursor and index documents in Elasticsearch
	for cursor.Next(context.Background()) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			panic(err)
		}

		// Indexing logic here
		// Convert document to JSON or use it as is (if it's already in JSON form)
		// Use esClient to index the document in Elasticsearch
	}

	if err := cursor.Err(); err != nil {
		panic(err)
	}

	fmt.Println("Finished indexing data from MongoDB to Elasticsearch")
}
