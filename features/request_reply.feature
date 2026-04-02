Feature: Asynchronous Request-Reply
  In order to handle long-running tasks
  As a Service X
  I want to send a task to Service Y and receive a confirmation through MBG when it's done

  Background:
    Given the broker is accessible at HTTP "http://localhost:8081" and gRPC "localhost:50051"

  Scenario: Service X receives a confirmation from Service Y via MBG
    Given the broker is started and executes "Background Start"
    And a mock server is listening at "http://localhost:9090/webhook"
    And a mock server is listening at "http://localhost:9091/api/callback"
    And the broker has the following registered targets:
      | name             | url                                |
      | worker-service   | http://localhost:9090/webhook      |
      | callback-service | http://localhost:9091/api/callback |
    When I push a task to "worker-service" with ID "TASK-REQ-001" and reply_to "callback-service"
    Then Service Y should receive the task
    And the mock server should trigger a reply to MBG for "callback-service"
    And Service X should eventually receive the "DONE" confirmation for task "TASK-REQ-001"
