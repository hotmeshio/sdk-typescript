-- Time-aware notification system for PostgreSQL
-- This system minimizes polling by using LISTEN/NOTIFY for time-based task awakening

-- Function to calculate the next awakening time from the sorted set
CREATE OR REPLACE FUNCTION {schema}.get_next_awakening_time(app_key TEXT)
RETURNS TIMESTAMP WITH TIME ZONE AS $$
DECLARE
    next_score DOUBLE PRECISION;
    next_time TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Get the earliest (lowest score) entry from the time range ZSET
    SELECT score INTO next_score
    FROM {schema}.task_schedules
    WHERE key = app_key
    AND (expiry IS NULL OR expiry > NOW())
    ORDER BY score ASC
    LIMIT 1;
    
    IF next_score IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Convert epoch milliseconds to timestamp
    next_time := to_timestamp(next_score / 1000.0);
    
    -- Only return if it's in the future
    IF next_time > NOW() THEN
        RETURN next_time;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to schedule a notification for the next awakening time
CREATE OR REPLACE FUNCTION {schema}.schedule_time_notification(
    app_id TEXT,
    new_awakening_time TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    channel_name TEXT;
    current_next_time TIMESTAMP WITH TIME ZONE;
    app_key TEXT;
BEGIN
    -- Build the time range key for this app
    app_key := app_id || ':time_range';
    channel_name := 'time_hooks_' || app_id;
    
    -- Get the current next awakening time
    current_next_time := {schema}.get_next_awakening_time(app_key);
    
    -- If we have a specific new awakening time, check if it's earlier
    IF new_awakening_time IS NOT NULL THEN
        IF current_next_time IS NULL OR new_awakening_time < current_next_time THEN
            current_next_time := new_awakening_time;
        END IF;
    END IF;
    
    -- If there's a next awakening time, schedule immediate notification
    -- The application will handle the timing logic
    IF current_next_time IS NOT NULL THEN
        PERFORM pg_notify(channel_name, json_build_object(
            'type', 'time_schedule_updated',
            'app_id', app_id,
            'next_awakening', extract(epoch from current_next_time) * 1000,
            'updated_at', extract(epoch from NOW()) * 1000
        )::text);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to notify when time hooks are ready
CREATE OR REPLACE FUNCTION {schema}.notify_time_hooks_ready(app_id TEXT)
RETURNS VOID AS $$
DECLARE
    channel_name TEXT;
BEGIN
    channel_name := 'time_hooks_' || app_id;
    
    PERFORM pg_notify(channel_name, json_build_object(
        'type', 'time_hooks_ready',
        'app_id', app_id,
        'ready_at', extract(epoch from NOW()) * 1000
    )::text);
END;
$$ LANGUAGE plpgsql;

-- Trigger function for when time hooks are added/updated
CREATE OR REPLACE FUNCTION {schema}.on_time_hook_change()
RETURNS TRIGGER AS $$
DECLARE
    app_id_extracted TEXT;
    awakening_time TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Extract app_id from the key (assumes format: app_id:time_range)
    app_id_extracted := split_part(NEW.key, ':time_range', 1);
    
    -- Convert the score (epoch milliseconds) to timestamp
    awakening_time := to_timestamp(NEW.score / 1000.0);
    
    -- Schedule notification for this new awakening time
    PERFORM {schema}.schedule_time_notification(app_id_extracted, awakening_time);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for when time hooks are removed
CREATE OR REPLACE FUNCTION {schema}.on_time_hook_remove()
RETURNS TRIGGER AS $$
DECLARE
    app_id_extracted TEXT;
BEGIN
    -- Extract app_id from the key
    app_id_extracted := split_part(OLD.key, ':time_range', 1);
    
    -- Recalculate and notify about the schedule update
    PERFORM {schema}.schedule_time_notification(app_id_extracted);
    
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create triggers on the sorted_set table for time hooks
-- Note: These will be created per app schema
-- Drop existing triggers first to avoid conflicts
DROP TRIGGER IF EXISTS trg_time_hook_insert ON {schema}.task_schedules;
DROP TRIGGER IF EXISTS trg_time_hook_update ON {schema}.task_schedules;
DROP TRIGGER IF EXISTS trg_time_hook_delete ON {schema}.task_schedules;

-- Create new triggers
CREATE TRIGGER trg_time_hook_insert
    AFTER INSERT ON {schema}.task_schedules
    FOR EACH ROW
    WHEN (NEW.key LIKE '%:time_range')
    EXECUTE FUNCTION {schema}.on_time_hook_change();

CREATE TRIGGER trg_time_hook_update
    AFTER UPDATE ON {schema}.task_schedules
    FOR EACH ROW
    WHEN (NEW.key LIKE '%:time_range')
    EXECUTE FUNCTION {schema}.on_time_hook_change();

CREATE TRIGGER trg_time_hook_delete
    AFTER DELETE ON {schema}.task_schedules
    FOR EACH ROW
    WHEN (OLD.key LIKE '%:time_range')
    EXECUTE FUNCTION {schema}.on_time_hook_remove(); 