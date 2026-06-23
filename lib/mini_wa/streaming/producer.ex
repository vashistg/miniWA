defmodule MiniWa.Streaming.Producer do
  require Logger

  @client :mini_wa_kafka
  @topic  "messages"

  # Publish a message struct to Kafka synchronously.
  # Returns :ok on success — caller sends tick-1 only after this.
  # Partition key = conversation_id so all messages in a chat land on the same
  # partition, preserving order.
  def publish(message) do
    # For group messages, conversation_id is already set to the group_id.
    # For 1:1, derive it from the two user IDs (sorted join).
    conv_id = Map.get(message, :conversation_id) ||
              MiniWa.DB.conversation_id(message.from, message.to)

    payload = Jason.encode!(%{
      id:              message.id,
      type:            Map.get(message, :type, "1:1"),
      from:            message.from,
      to:              message.to,
      content:         message.content,
      client_id:       message.client_id,
      sent_at:         message.sent_at,
      conversation_id: conv_id
    })

    key = conv_id

    Logger.info("""
    [Kafka][Producer] ─── PUBLISH ───────────────────────────
      topic      : #{@topic}
      key        : #{key}
      message_id : #{message.id}
      from→to    : #{message.from} → #{message.to}
    """)

    case :brod.produce_sync(@client, @topic, :hash, key, payload) do
      :ok ->
        Logger.info("[Kafka][Producer] ✓ confirmed by broker | message_id=#{message.id}")
        :ok

      {:error, reason} ->
        Logger.error("[Kafka][Producer] ✗ publish failed | message_id=#{message.id} | #{inspect(reason)}")
        {:error, reason}
    end
  end
end
