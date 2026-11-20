DROP TRIGGER batch_progress ON notifications;
DROP FUNCTION maintain_batch_counts();
ALTER TABLE notification_batches DROP COLUMN completed_count;
