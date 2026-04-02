Feature: Message Broker Reliability and Persistence
  As a developer
  I want a message broker that ensures data durability and protects against system failures
  So that I can build reliable distributed systems

  Background:
    Given the broker is accessible at HTTP "http://localhost:8081" and gRPC "localhost:50051"
    And the configuration has threshold set to 3
    And the configuration has timeout set to 5 seconds

  Scenario: Happy Path - Message Persistence and Delivery
    Given the broker is initialized with an empty queue
    And the circuit breaker is closed
    When a producer pushes a message "TX-001" with payload "Hello"
    Then the message "TX-001" should be stored in "../data/messages/TX-001.json"
    And the message should be available in the memory queue
    And when a consumer pops a message
    Then the consumer should receive message "TX-001"
    And the file "./data/messages/TX-001.json" should be deleted

  Scenario: Circuit Breaker - Failure Threshold Reached
    Given the circuit breaker threshold is set to 3
    And the circuit breaker is closed
    When 3 consecutive push attempts fail due to storage errors
    Then the circuit breaker should transition to "Open" state
    And subsequent push attempts should fail immediately with "circuit is open" error

  Scenario: System Recovery - Message Re-hydration
    Given the broker is stopped
    And there are messages "TX-101" and "TX-102" on disk in "../data/messages/"
    When the broker is started and executes "Recover"
    Then the memory queue should contain 2 messages
    And the messages "TX-101" and "TX-102" should be available for consumption in the correct order

  @cb-heal
  Scenario: Circuit Breaker - Self-Healing
    Given the circuit breaker is "Open"
    And the timeout of 5 seconds has passed
    When a new push attempt is "Successful"
    Then the circuit breaker should transition to "Half-Open"
    And then finally transition to "Closed"
    And the message should be persisted and queued normally

  Scenario: Happy Path - gRPC Communication
    When the producer sends message "GRPC-001" with payload "Hello gRPC" via gRPC
    Then the message "GRPC-001" should be stored in "../data/messages/GRPC-001.json"
    And when the consumer requests a message via gRPC
    Then the consumer should receive message "GRPC-001" via gRPC

  Scenario: Happy Path - HTTP REST API
    When the producer sends message "HTTP-001" with payload "Hello HTTP" via POST "/api/messages"
    Then the server should respond with status 201
    And the message "HTTP-001" should be stored in "../data/messages/HTTP-001.json"
    When the consumer requests a message via GET "/api/messages"
    Then the consumer should receive message "HTTP-001" via HTTP

  Scenario: Structured Payload - JSON Handling
    When the producer sends the following JSON payload via gRPC:
      """
      {
        "id": "JSON-001",
        "payload": {
          "event": "user_signup",
          "data": {"user_id": 42, "role": "admin"}
        }
      }
      """
    Then the message "JSON-001" should be stored in "../data/messages/JSON-001.json"
    And when the consumer requests a message via gRPC
    Then the consumer should receive message "JSON-001" via gRPC

  Scenario: Structured Payload - JSON Handling via HTTP
    When the producer sends the following JSON payload via POST "/api/messages":
      """
      {
        "id": "JSON-HTTP-001",
        "payload": {
          "event": "user_signup",
          "data": {"user_id": 42, "role": "admin"}
        }
      }
      """
    Then the server should respond with status 201
    And the message "JSON-HTTP-001" should be stored in "../data/messages/JSON-HTTP-001.json"
    When the consumer requests a message via GET "/api/messages"
    Then the consumer should receive message "JSON-HTTP-001" via HTTP

  Scenario: Dashboard - Real-time Visualization
    Given the dashboard is accessible
    When a producer pushes a message "DASH-001" via gRPC
    Then the dashboard stats should show queue size as 1
    And when a consumer pops a message via HTTP
    Then the dashboard stats should show queue size as 0

  Scenario: Active Delivery - Automatic Push to Target
    Given the broker has a registered target "http://localhost:9590/webhook"
    And a mock server is listening at "http://localhost:9590/webhook"
    When a producer pushes a message "PUSH-001" with payload "AutoDelivery"
    Then the mock server should receive message "PUSH-001"
    And the message "PUSH-001" should eventually be deleted from the queue

  Scenario: Active Delivery - Exponential Backoff on Failure
    Given the broker has a registered target "http://localhost:9591/fail"
    And a mock server at "http://localhost:9591/fail" returns 500 error
    When a producer pushes a message "RETRY-001" with payload "FailTarget"
    Then the message "RETRY-001" should stay in the queue
    And its "RetryCount" should be 1
    And its "NextRetry" should be set according to exponential backoff

  Scenario: Active Delivery via gRPC
    Given the broker is initialized with an empty queue
    And a mock server is listening at "grpc://localhost:55052"
    And the broker has a registered target "grpc://localhost:55052"
    When a producer pushes a message "TX-GRPC" with payload "Hello gRPC"
    Then the mock server should receive message "TX-GRPC"
    And the message "TX-GRPC" should eventually be deleted from the queue

  Scenario: Dead Letter Queue - Max Retries Reached
    Given the broker has a registered target "http://localhost:9592/dead"
    And a mock server at "http://localhost:9592/dead" always returns 500 error
    And the configuration has max_retries set to 3
    When a producer pushes a message "DEAD-001" with payload "ToDLQ"
    Then the message "DEAD-001" should eventually be moved to DLQ folder "../data/dead_letter/DEAD-001.json"
    And the message "DEAD-001" should be removed from the main queue
    And the dashboard stats should show dlq size as 1
    And a mock server at "http://localhost:9092" always returns 500 error

  Scenario: Selective Routing - Delivery to a Named Target
    Given the broker has the following registered targets:
      | Name          | URL                            |
      | auth-service  | http://localhost:9590/webhook  |
      | notify-system | http://localhost:9591/fail     |
    And a mock server is listening at "http://localhost:9590/webhook"
    And a mock server is listening at "http://localhost:9591/fail"
    When a producer pushes a message "SELECT-001" with target "auth-service"
    Then the mock server should receive message "SELECT-001"
    And the mock server at "http://localhost:9591/fail" should NOT receive message "SELECT-001"
    And the message "SELECT-001" should eventually be deleted from the queue
