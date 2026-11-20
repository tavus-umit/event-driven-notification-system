CREATE TABLE notification_attempt_stats (
 channel channel_type NOT NULL, outcome attempt_status NOT NULL, bucket_ms int NOT NULL,
 samples bigint NOT NULL, sum_ms bigint NOT NULL,
 PRIMARY KEY (channel,outcome,bucket_ms)
);
CREATE FUNCTION attempt_bucket(ms integer) RETURNS integer IMMUTABLE LANGUAGE SQL AS $$
 SELECT coalesce((SELECT min(b) FROM unnest(ARRAY[5,10,25,50,100,250,500,1000,2500,5000,10000,30000]) b WHERE ms<=b),2147483647)
$$;
-- Block concurrent inserts until both the backfill and its replacement trigger are installed.
LOCK TABLE delivery_attempts IN SHARE ROW EXCLUSIVE MODE;
INSERT INTO notification_attempt_stats
 SELECT n.channel,a.status,attempt_bucket(a.duration_ms),count(*),sum(a.duration_ms)
 FROM delivery_attempts a JOIN notifications n ON n.id=a.notification_id GROUP BY 1,2,3;
CREATE FUNCTION record_attempt_stats() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO notification_attempt_stats
 SELECT n.channel,NEW.status,attempt_bucket(NEW.duration_ms),1,NEW.duration_ms FROM notifications n WHERE n.id=NEW.notification_id
 ON CONFLICT (channel,outcome,bucket_ms) DO UPDATE SET samples=notification_attempt_stats.samples+1,sum_ms=notification_attempt_stats.sum_ms+EXCLUDED.sum_ms;
 RETURN NEW;
END $$;
CREATE TRIGGER audit_metrics AFTER INSERT ON delivery_attempts FOR EACH ROW EXECUTE FUNCTION record_attempt_stats();
