package main

import (
	"context"
	"fmt"
	"os"
	"bytes"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		// Convert BSON to JSON
		jsonBytes, err := bson.MarshalExtJSON(document, false, false)
		if err != nil {
			panic(err)
		}

		// Elasticsearch expects document IDs to index data
		// Assuming the MongoDB document has an "_id" field that can be used as the document ID
		docID := document["_id"].(primitive.ObjectID).Hex()

		// Index the JSON document in Elasticsearch
		// The index name should be predefined or created before this operation
		res, err := esClient.Index(
			"index_name",                         // Index name
			bytes.NewReader(jsonBytes),           // Document body
			esClient.Index.WithDocumentID(docID), // Document ID
			esClient.Index.WithRefresh("true"),   // Refresh the index after the operation
		)
		if err != nil {
			// Handle error
			fmt.Printf("Error indexing document ID %s: %s\n", docID, err)
			continue
		}
		defer res.Body.Close()

		if res.IsError() {
			// Parse the response body to get the error message
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				// Error parsing the response body
				fmt.Printf("Error parsing the response body: %s\n", err)
			} else {
				// Elasticsearch error message
				fmt.Printf("[%s] %s: %s\n", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
			}
		} else {
			// Document indexed successfully
			fmt.Printf("Document ID %s indexed successfully.\n", docID)
		}
	}

	if err := cursor.Err(); err != nil {
		panic(err)
	}

	fmt.Println("Finished indexing data from MongoDB to Elasticsearch")
}
