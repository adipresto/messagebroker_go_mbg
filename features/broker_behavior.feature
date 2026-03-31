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

  Scenario: Dashboard - Real-time Visualization
    Given the dashboard is accessible
    When a producer pushes a message "DASH-001" via gRPC
    Then the dashboard stats should show queue size as 1
    And when a consumer pops a message via HTTP
    Then the dashboard stats should show queue size as 0
