package main

import (
	"event-driven-notification-system/internal/app"
	"event-driven-notification-system/internal/scheduler"
)

func main() { app.Main(scheduler.Run) }
