/**
 * Time-aware notification system for PostgreSQL.
 * Triggers on task_schedules fire LISTEN/NOTIFY so the scout can
 * sleep precisely until the next timer expires instead of polling.
 */
export function getTimeNotifySql(schema: string): string {
  return `
-- Get the earliest future awakening time from task_schedules
CREATE OR REPLACE FUNCTION ${schema}.get_next_awakening_time()
RETURNS TIMESTAMP WITH TIME ZONE AS $$
DECLARE
    next_score DOUBLE PRECISION;
BEGIN
    SELECT score INTO next_score
    FROM ${schema}.task_schedules
    WHERE score > extract(epoch from NOW()) * 1000
    ORDER BY score ASC
    LIMIT 1;

    IF next_score IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN to_timestamp(next_score / 1000.0);
END;
$$ LANGUAGE plpgsql;

-- Schedule a notification for the next awakening time
CREATE OR REPLACE FUNCTION ${schema}.schedule_time_notification(
    app_id TEXT,
    new_awakening_time TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    channel_name TEXT;
    current_next_time TIMESTAMP WITH TIME ZONE;
BEGIN
    channel_name := 'time_hooks_' || app_id;
    current_next_time := ${schema}.get_next_awakening_time();

    IF new_awakening_time IS NOT NULL THEN
        IF current_next_time IS NULL OR new_awakening_time < current_next_time THEN
            current_next_time := new_awakening_time;
        END IF;
    END IF;

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

-- Notify when time hooks are ready
CREATE OR REPLACE FUNCTION ${schema}.notify_time_hooks_ready(app_id TEXT)
RETURNS VOID AS $$
BEGIN
    PERFORM pg_notify(
        'time_hooks_' || app_id,
        json_build_object(
            'type', 'time_hooks_ready',
            'app_id', app_id,
            'ready_at', extract(epoch from NOW()) * 1000
        )::text
    );
END;
$$ LANGUAGE plpgsql;

-- Trigger function for INSERT/UPDATE on task_schedules
CREATE OR REPLACE FUNCTION ${schema}.on_time_hook_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM ${schema}.schedule_time_notification(
        '${schema}',
        to_timestamp(NEW.score / 1000.0)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for DELETE on task_schedules
CREATE OR REPLACE FUNCTION ${schema}.on_time_hook_remove()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM ${schema}.schedule_time_notification('${schema}');
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate triggers
DROP TRIGGER IF EXISTS trg_time_hook_insert ON ${schema}.task_schedules;
DROP TRIGGER IF EXISTS trg_time_hook_update ON ${schema}.task_schedules;
DROP TRIGGER IF EXISTS trg_time_hook_delete ON ${schema}.task_schedules;

CREATE TRIGGER trg_time_hook_insert
    AFTER INSERT ON ${schema}.task_schedules
    FOR EACH ROW
    EXECUTE FUNCTION ${schema}.on_time_hook_change();

CREATE TRIGGER trg_time_hook_update
    AFTER UPDATE ON ${schema}.task_schedules
    FOR EACH ROW
    EXECUTE FUNCTION ${schema}.on_time_hook_change();

CREATE TRIGGER trg_time_hook_delete
    AFTER DELETE ON ${schema}.task_schedules
    FOR EACH ROW
    EXECUTE FUNCTION ${schema}.on_time_hook_remove();
`;
}
