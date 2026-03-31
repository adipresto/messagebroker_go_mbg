package models

type Message[T any] struct {
	ID         string `json:"id" validate:"required"`
	Payload    T      `json:"payload"`
	CreatedAt  int64  `json:"created_at"`
	RetryCount int    `json:"retry_count"`
	NextRetry  int64  `json:"next_retry"`
}

type QueueStats struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}
