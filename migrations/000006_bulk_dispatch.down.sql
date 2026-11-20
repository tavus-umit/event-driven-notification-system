DROP TRIGGER notifications_bulk_dispatch ON notifications;
DROP FUNCTION dispatch_inserted_notifications();
DROP TRIGGER notifications_dispatch ON notifications;
CREATE TRIGGER notifications_dispatch AFTER INSERT OR UPDATE OF status ON notifications FOR EACH ROW EXECUTE FUNCTION notification_transition();
