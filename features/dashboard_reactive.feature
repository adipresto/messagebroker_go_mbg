Feature: Dashboard Reactive Updates
  As a developer
  I want the dashboard to update automatically when targets change
  So that I have real-time visibility without manual refresh

  Background:
    Given the broker is accessible at HTTP "http://localhost:8081" and gRPC "localhost:50051"

  Scenario: Dashboard - Reactive Target Updates
    Given the dashboard is accessible via WebSocket
    When a new target "dynamic-target" with URL "http://localhost:9999" is registered via POST "/api/targets"
    Then the dashboard should receive a WebSocket message containing the new target "dynamic-target"
