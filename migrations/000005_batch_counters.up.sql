-- A replay can move a terminal notification back into the active batch.
ALTER TABLE notification_batches ADD COLUMN completed_count integer NOT NULL DEFAULT 0;
LOCK TABLE notifications IN SHARE ROW EXCLUSIVE MODE;
UPDATE notification_batches b SET success_count=t.delivered,failure_count=t.failed,completed_count=t.done,
 updated_at=t.updated,status=CASE WHEN t.done<b.total_count THEN 'PROCESSING' WHEN t.failed>0 THEN 'PARTIALLY_FAILED' ELSE 'COMPLETED' END
FROM (SELECT batch_id,count(*) FILTER(WHERE status='DELIVERED') delivered,count(*) FILTER(WHERE status='FAILED') failed,
 count(*) FILTER(WHERE status IN ('DELIVERED','FAILED','CANCELLED')) done,max(updated_at) updated
 FROM notifications WHERE batch_id IS NOT NULL GROUP BY batch_id) t WHERE b.id=t.batch_id;

CREATE FUNCTION maintain_batch_counts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
 done_delta integer := (NEW.status IN ('FAILED','DELIVERED','CANCELLED'))::integer - (OLD.status IN ('FAILED','DELIVERED','CANCELLED'))::integer;
 failed_delta integer := (NEW.status='FAILED')::integer - (OLD.status='FAILED')::integer;
 success_delta integer := (NEW.status='DELIVERED')::integer - (OLD.status='DELIVERED')::integer;
BEGIN
 IF done_delta<>0 OR failed_delta<>0 OR success_delta<>0 THEN
  UPDATE notification_batches SET
   completed_count=completed_count+done_delta,failure_count=failure_count+failed_delta,success_count=success_count+success_delta,
   updated_at=greatest(updated_at,NEW.updated_at),
   status=CASE WHEN completed_count+done_delta<total_count THEN 'PROCESSING'
               WHEN failure_count+failed_delta>0 THEN 'PARTIALLY_FAILED' ELSE 'COMPLETED' END
   WHERE id=NEW.batch_id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER batch_progress AFTER UPDATE OF status ON notifications FOR EACH ROW
 WHEN (NEW.batch_id IS NOT NULL AND OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION maintain_batch_counts();
