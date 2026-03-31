package models

type Message[T any] struct {
	ID        string `json:"id" validate:"required"`
	Payload   T      `json:"payload"`
	CreatedAt int64  `json:"created_at"`
}

type QueueStats struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
}
