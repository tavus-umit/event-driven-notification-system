ALTER TYPE notification_status ADD VALUE IF NOT EXISTS 'RETRY_PENDING';
ALTER TABLE notifications ADD COLUMN correlation_id text NOT NULL DEFAULT '',
 ADD COLUMN traceparent text NOT NULL DEFAULT '', ADD COLUMN claim_id uuid,
 ADD COLUMN lease_until timestamptz, ADD COLUMN due_at timestamptz;
CREATE INDEX notifications_expired_leases ON notifications(lease_until) WHERE status='PROCESSING';
CREATE INDEX notifications_listing ON notifications(created_at DESC,id DESC);
-- Expiration belongs to the receipt. A permanent unique index would block reuse after 24h.
DROP INDEX idx_notifications_idempotency_key;
CREATE TABLE notification_outbox (
 seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 notification_id uuid NOT NULL, batch_id uuid, channel channel_type NOT NULL,
 priority priority_type NOT NULL, recipient text NOT NULL, previous_status text NOT NULL,
 current_status text NOT NULL, due_at timestamptz, retry_count integer NOT NULL, reason text NOT NULL,
 correlation_id text NOT NULL, traceparent text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION notification_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.status=NEW.status THEN RETURN NEW; END IF;
 INSERT INTO notification_outbox(notification_id,batch_id,channel,priority,recipient,previous_status,current_status,due_at,retry_count,reason,correlation_id,traceparent)
 VALUES(NEW.id,NEW.batch_id,NEW.channel,NEW.priority,NEW.recipient,CASE WHEN TG_OP='INSERT' THEN 'PENDING' ELSE OLD.status::text END,NEW.status::text,COALESCE(NEW.due_at,NEW.scheduled_at),NEW.retry_count,COALESCE(NEW.error_message,''),NEW.correlation_id,NEW.traceparent);
 RETURN NEW;
END $$;
CREATE TRIGGER notifications_dispatch AFTER INSERT OR UPDATE OF status ON notifications FOR EACH ROW EXECUTE FUNCTION notification_transition();

-- Route active records when upgrading a database already populated at version 1.
UPDATE notifications SET correlation_id=uuid_generate_v4()::text WHERE correlation_id='';
INSERT INTO notification_outbox(notification_id,batch_id,channel,priority,recipient,previous_status,current_status,due_at,retry_count,reason,correlation_id,traceparent)
 SELECT id,batch_id,channel,priority,recipient,'PENDING',status::text,scheduled_at,retry_count,'',correlation_id,traceparent
 FROM notifications WHERE status IN ('QUEUED','SCHEDULED');
UPDATE notifications SET status=CASE WHEN scheduled_at>now() THEN 'SCHEDULED'::notification_status ELSE 'QUEUED'::notification_status END,
 updated_at=now() WHERE status IN ('PENDING','PROCESSING');
