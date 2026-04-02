Feature: Payload Headers and Authorization
  As a developer
  I want to send custom headers with my messages
  So that I can provide authorization to target systems

  Background:
    Given the broker is accessible at HTTP "http://localhost:8081" and gRPC "localhost:50051"
    And the configuration has threshold set to 3
    And the circuit breaker is "Closed"
    And the broker is initialized with an empty queue

  Scenario: Delivering a message with custom HTTP headers
    Given a mock server is listening at "http://localhost:9595/webhook"
    And the broker has the following registered targets:
      | name         | url                             |
      | auth-target  | http://localhost:9595/webhook   |
    When the producer sends the following JSON payload via POST "/api/messages":
      """
      {
        "id": "MSG-HDR-001",
        "target": "auth-target",
        "payload": {"data": "secure-content"},
        "headers": {
          "X-Custom-Auth": "SecretToken123",
          "X-Service-ID": "Broker-01"
        }
      }
      """
    Then the mock server at "http://localhost:9595/webhook" should receive message "MSG-HDR-001" with headers:
      | header-name   | header-value     |
      | X-Custom-Auth | SecretToken123   |
      | X-Service-ID  | Broker-01        |

  Scenario: Delivering a message with default target headers
    Given a mock server is listening at "http://localhost:9596/webhook"
    And the broker has the following registered targets with headers:
      | name         | url                             | header-name    | header-value |
      | default-auth | http://localhost:9596/webhook   | Authorization  | Bearer ABC   |
    When a producer pushes a message with payload "MSG-DEF-001" and data "test" to "default-auth"
    Then the mock server at "http://localhost:9596/webhook" should receive message "MSG-DEF-001" with headers:
      | header-name   | header-value |
      | Authorization | Bearer ABC   |

  Scenario: Message headers override target default headers
    Given a mock server is listening at "http://localhost:9597/webhook"
    And the broker has the following registered targets with headers:
      | name         | url                             | header-name    | header-value |
      | override-auth| http://localhost:9597/webhook   | Authorization  | Bearer OLD   |
    When the producer sends the following JSON payload via POST "/api/messages":
      """
      {
        "id": "MSG-OVERRIDE-001",
        "target": "override-auth",
        "payload": "data",
        "headers": {
          "Authorization": "Bearer NEW"
        }
      }
      """
    Then the mock server at "http://localhost:9597/webhook" should receive message "MSG-OVERRIDE-001" with headers:
      | header-name   | header-value |
      | Authorization | Bearer NEW   |
  Scenario: Target receives only the payload in the request body
    Given a mock server is listening at "http://localhost:9598/webhook"
    And the broker has the following registered targets:
      | name         | url                             |
      | clean-target | http://localhost:9598/webhook   |
    When the producer sends the following JSON payload via POST "/api/messages":
      """
      {
        "id": "MSG-CLEAN-001",
        "target": "clean-target",
        "payload": {"info": "this is the only thing in body"},
        "headers": {"X-Test": "Value"}
      }
      """
    Then the mock server at "http://localhost:9598/webhook" should receive message "MSG-CLEAN-001" with headers:
      | header-name   | header-value |
      | X-Message-ID  | MSG-CLEAN-001|
      | X-Test        | Value        |
    And the mock server at "http://localhost:9598/webhook" should receive only the payload of message "MSG-CLEAN-001"
