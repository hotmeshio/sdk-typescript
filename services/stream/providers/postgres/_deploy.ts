export class DeployService {
  constructor() {}

  //add namespace to the other tables (multitenancy)
  async deploy(namespace: string): Promise<void> {
    //deploy the tables
    const query = `
-- messages table
CREATE TABLE IF NOT EXISTS messages${namespace} (
    stream VARCHAR(255) NOT NULL,
    message_id BIGSERIAL PRIMARY KEY,
    message JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_stream_message_id ON messages (stream, message_id);

-- consumer_groups table
CREATE TABLE IF NOT EXISTS consumer_groups (
    stream VARCHAR(255) NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    last_message_id BIGINT DEFAULT 0,
    PRIMARY KEY (stream, group_name)
);

CREATE INDEX IF NOT EXISTS idx_consumer_groups_stream_group ON consumer_groups (stream, group_name);

-- pending_messages table
CREATE TABLE IF NOT EXISTS pending_messages (
    stream VARCHAR(255) NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    consumer_name VARCHAR(255) NOT NULL,
    message_id BIGINT NOT NULL,
    delivered_at TIMESTAMPTZ DEFAULT NOW(),
    delivery_count INTEGER DEFAULT 1,
    PRIMARY KEY (stream, group_name, message_id)
);

CREATE INDEX IF NOT EXISTS idx_pending_messages_consumer ON pending_messages (stream, group_name, consumer_name);
CREATE INDEX IF NOT EXISTS idx_pending_messages_message_id ON pending_messages (stream, group_name, message_id);
`;
  }
}

export default DeployService;
