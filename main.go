package main

import (
	// Kafka_Deps
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"

	// Redis_Deps
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var redisAddress = os.Getenv("REDIS_ADDRESS")
var kafkaAddress = os.Getenv("KAFKA_ADDRESS")
var rdb *redis.Client

func buildRedis() {
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}
	rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddress, // Redis address
		Password: "",           // No password set
		DB:       0,            // Use default DB
	})

	fmt.Printf("Connecting to Redis: PING\n")
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	fmt.Printf("Connected to Redis: %s\n", pong)
}

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func saveRedis(rdb *redis.Client, order Order) {

	val, err := rdb.Get(ctx, order.CustomerName).Result()
	if err == redis.Nil {
		err = rdb.Set(ctx, order.CustomerName, order.CoffeeType, 10*time.Minute).Err()
		if err != nil {
			log.Fatalf("Failed to set key: %v\n", err)
		}
		fmt.Printf("Key '%s' set successfully.\n", order.CustomerName)
	} else {
		log.Printf("Key '%s' already exists with Value '%s'\n", order.CustomerName, val)
	}
}

func main() {

	buildRedis()
	consumer := buildKafkaConnection()

	msgCount := 0

	// 2. Handle OS signals - used to stop the process.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Create a GoRoutine to run the consumer/worker.
	doneCH := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received order count %d: / Topic(%s) / Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
				var order Order
				if err := json.Unmarshal(msg.Value, &order); err != nil {
					log.Printf("Error unmarshalling order: %v\n", err)
				}
				saveRedis(rdb, order)
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCH <- struct{}{}
			}
		}
	}()

	<-doneCH
	fmt.Println("processed", msgCount, "messages")

	// 4. CLose the consumer on exit.

	defer consumer.Close()
}

func buildKafkaConnection() sarama.PartitionConsumer {
	topic := "coffee_orders"

	// 1. Create a new consumer and start it.
	brokers := []string{"localhost:9092"}
	if kafkaAddress != "" {
		brokers = []string{kafkaAddress}
	}

	// Tries to connect again to kafka if it not starts in time
	var worker sarama.Consumer
	var err error
	for i := 0; i < 20; i++ {
		worker, err = ConnectConsumer(brokers)
		if err == nil {
			break
		}
		fmt.Printf("Kafka not ready: %v. Retrying in 3 seconds...\n", err)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Kafka Consumer started")
	}

	fmt.Println("Consumer started")
	return consumer
}

func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}
