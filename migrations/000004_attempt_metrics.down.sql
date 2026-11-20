DROP TRIGGER audit_metrics ON delivery_attempts;
DROP FUNCTION record_attempt_stats();
DROP TABLE notification_attempt_stats;
DROP FUNCTION attempt_bucket(integer);
