package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Get configurations from the environment variables
	mongoDBURI := os.Getenv("MONGODB_URI")
	elasticSearchPort := os.Getenv("ELASTICSEARCH_PORT")
	mongoDBName := os.Getenv("MONGO_DB_NAME")
	mongoCollectionName := os.Getenv("MONGO_COLLECTION_NAME")
	elasticsearchIndexName := os.Getenv("ELASTICSEARCH_INDEX_NAME")

	// Connect to MongoDB
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoDBURI))
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}

	// Ping MongoDB to verify connection
	if err := mongoClient.Ping(context.TODO(), nil); err != nil {
		log.Fatalf("Error pinging MongoDB: %v", err)
	}
	fmt.Println("Connected to MongoDB!")

	// Connect to Elasticsearch
	elasticSearchAddr := fmt.Sprintf("http://localhost:%s", elasticSearchPort)
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticSearchAddr},
	})
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	// Ping Elasticsearch to verify connection
	if _, err := esClient.Info(); err != nil {
		log.Fatalf("Error pinging Elasticsearch: %v", err)
	}
	fmt.Println("Connected to Elasticsearch!")

	// Define the MongoDB collection to index
	collection := mongoClient.Database(mongoDBName).Collection(mongoCollectionName)

	// Retrieve documents from the MongoDB collection
	cursor, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		log.Fatalf("Error finding documents: %v", err)
	}
	defer cursor.Close(context.Background())

	// Iterate through the cursor and index documents in Elasticsearch
	for cursor.Next(context.Background()) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			log.Printf("Error decoding document: %v", err)
			continue
		}

		// Get the hex string representation of the ObjectID
		var docID string
		if oid, ok := document["_id"].(primitive.ObjectID); ok {
			docID = oid.Hex()
		} else {
			log.Printf("Error asserting _id to ObjectID for document: %v", document)
			continue
		}
		delete(document, "_id")
		jsonBytes, err := json.Marshal(document)
		if err != nil {
			log.Printf("Error marshaling document: %v", err)
			continue
		}

		// Use the _id field as the Elasticsearch document ID

		// Index the JSON document in Elasticsearch
		res, err := esClient.Index(
			elasticsearchIndexName,               // Index name
			bytes.NewReader(jsonBytes),           // Document body
			esClient.Index.WithDocumentID(docID), // Document ID
			esClient.Index.WithRefresh("true"),   // Refresh the index after the operation
		)
		if err != nil {
			log.Printf("Error indexing document ID %s: %v", docID, err)
			continue
		}
		if res.IsError() {
			// Parse the response body to get the error message
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				log.Printf("Error parsing the response body: %v", err)
			} else {
				// Elasticsearch error message
				log.Printf("[%s] %s: %s\n", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
			}
		} else {
			// Document indexed successfully
			log.Printf("Document ID %s indexed successfully.", docID)
		}
		res.Body.Close()
	}

	if err := cursor.Err(); err != nil {
		log.Fatalf("Error with cursor: %v", err)
	}

	fmt.Println("Finished indexing data from MongoDB to Elasticsearch")
}
