DROP TRIGGER notifications_dispatch ON notifications;
CREATE TRIGGER notifications_dispatch AFTER UPDATE OF status ON notifications
 FOR EACH ROW EXECUTE FUNCTION notification_transition();

CREATE FUNCTION dispatch_inserted_notifications() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO notification_outbox(notification_id,batch_id,channel,priority,recipient,previous_status,current_status,due_at,retry_count,reason,correlation_id,traceparent)
 SELECT id,batch_id,channel,priority,recipient,'PENDING',status::text,coalesce(due_at,scheduled_at),retry_count,coalesce(error_message,''),correlation_id,traceparent
 FROM accepted;
 RETURN NULL;
END $$;
CREATE TRIGGER notifications_bulk_dispatch AFTER INSERT ON notifications
 REFERENCING NEW TABLE AS accepted FOR EACH STATEMENT EXECUTE FUNCTION dispatch_inserted_notifications();
