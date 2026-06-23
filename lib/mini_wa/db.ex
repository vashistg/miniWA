defmodule MiniWa.DB do
  require Logger

  # ─── Conversation ID (1:1 only) ────────────────────────────────────────────
  def conversation_id(user_a, user_b) do
    [user_a, user_b] |> Enum.sort() |> Enum.join(":")
  end

  # ─── Users ─────────────────────────────────────────────────────────────────

  def register_user(user_id) do
    Logger.info("[DB] Registering user=#{user_id}")
    case Xandra.execute(MiniWa.Xandra,
           "INSERT INTO mini_wa.users (user_id, registered_at) VALUES (?, toTimestamp(now()))",
           [{"text", user_id}]) do
      {:ok, _} -> :ok
      {:error, reason} ->
        Logger.error("[DB] register_user failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def list_users do
    case Xandra.execute(MiniWa.Xandra, "SELECT user_id FROM mini_wa.users", []) do
      {:ok, page} -> {:ok, Enum.map(page, fn row -> row["user_id"] end)}
      {:error, reason} -> {:error, reason}
    end
  end

  # ─── Groups ────────────────────────────────────────────────────────────────

  def create_group(group_id, name, created_by) do
    Logger.info("[DB] Creating group | id=#{group_id} name=#{name} creator=#{created_by}")
    with {:ok, _} <- Xandra.execute(MiniWa.Xandra,
           "INSERT INTO mini_wa.groups (group_id, name, created_by, created_at) VALUES (?, ?, ?, toTimestamp(now()))",
           [{"text", group_id}, {"text", name}, {"text", created_by}]),
         # Creator is always a full-history member (share_from = 0)
         :ok <- add_group_member(group_id, name, created_by, created_by, 0) do
      Logger.info("[DB] ✓ Group created | #{group_id}")
      :ok
    end
  end

  def add_group_member(group_id, group_name, user_id, added_by, share_from_ms) do
    Logger.info("[DB] Adding #{user_id} to group #{group_id} | share_from=#{share_from_ms}")
    with {:ok, _} <- Xandra.execute(MiniWa.Xandra,
           "INSERT INTO mini_wa.group_members (group_id, user_id, added_by, added_at, share_from) VALUES (?, ?, ?, toTimestamp(now()), ?)",
           [{"text", group_id}, {"text", user_id}, {"text", added_by}, {"bigint", share_from_ms}]),
         {:ok, _} <- Xandra.execute(MiniWa.Xandra,
           "INSERT INTO mini_wa.user_groups (user_id, group_id, group_name) VALUES (?, ?, ?)",
           [{"text", user_id}, {"text", group_id}, {"text", group_name}]) do
      Logger.info("[DB] ✓ #{user_id} added to #{group_id}")
      :ok
    else
      {:error, reason} ->
        Logger.error("[DB] add_group_member failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def list_group_members(group_id) do
    case Xandra.execute(MiniWa.Xandra,
           "SELECT user_id, share_from FROM mini_wa.group_members WHERE group_id = ?",
           [{"text", group_id}]) do
      {:ok, page} ->
        {:ok, Enum.map(page, fn row ->
          %{user_id: row["user_id"], share_from: row["share_from"] || 0}
        end)}
      {:error, reason} -> {:error, reason}
    end
  end

  def remove_group_member(group_id, user_id) do
    Logger.info("[DB] Removing #{user_id} from group #{group_id}")
    with {:ok, _} <- Xandra.execute(MiniWa.Xandra,
           "DELETE FROM mini_wa.group_members WHERE group_id = ? AND user_id = ?",
           [{"text", group_id}, {"text", user_id}]),
         {:ok, _} <- Xandra.execute(MiniWa.Xandra,
           "DELETE FROM mini_wa.user_groups WHERE user_id = ? AND group_id = ?",
           [{"text", user_id}, {"text", group_id}]) do
      Logger.info("[DB] ✓ #{user_id} removed from #{group_id}")
      :ok
    else
      {:error, reason} ->
        Logger.error("[DB] remove_group_member failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  def list_user_groups(user_id) do
    case Xandra.execute(MiniWa.Xandra,
           "SELECT group_id, group_name FROM mini_wa.user_groups WHERE user_id = ?",
           [{"text", user_id}]) do
      {:ok, page} ->
        {:ok, Enum.map(page, fn row ->
          %{group_id: row["group_id"], name: row["group_name"]}
        end)}
      {:error, reason} -> {:error, reason}
    end
  end

  # Fetch messages in a group since a Unix timestamp in ms.
  # ALLOW FILTERING is OK here — it only scans within the group's partition.
  def fetch_group_history(group_id, since_ms) do
    Logger.info("[DB] Fetching history | group=#{group_id} since_ms=#{since_ms}")
    case Xandra.execute(MiniWa.Xandra,
           """
           SELECT message_id, sender_id, content, sent_at_ms
           FROM mini_wa.messages
           WHERE conversation_id = ? AND sent_at_ms >= ?
           ALLOW FILTERING
           """,
           [{"text", group_id}, {"bigint", since_ms}]) do
      {:ok, page} ->
        {:ok, Enum.map(page, fn row ->
          %{
            id:              row["message_id"],
            from:            row["sender_id"],
            to:              group_id,
            content:         row["content"],
            conversation_id: group_id,
            type:            "group",
            sent_at:         unix_ms_to_iso(row["sent_at_ms"])
          }
        end)}
      {:error, reason} ->
        Logger.error("[DB] fetch_group_history failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # ─── Messages ──────────────────────────────────────────────────────────────

  # 1:1 message — write to log + undelivered queue if recipient offline
  def persist_message(message, recipient_online?) do
    conv_id = conversation_id(message.from, message.to)
    Logger.info("[DB] Writing 1:1 message | id=#{message.id} conv=#{conv_id}")
    with :ok <- insert_message(conv_id, message) do
      if recipient_online?, do: :ok, else: insert_undelivered(conv_id, message)
    end
  end

  # Group message — write to log once; caller handles per-member delivery/queuing
  def persist_group_message(message) do
    Logger.info("[DB] Writing group message | id=#{message.id} group=#{message.conversation_id}")
    insert_message(message.conversation_id, message)
  end

  # Queue one undelivered entry for a specific member of a group
  def queue_undelivered_for_member(recipient_id, message) do
    insert_undelivered(message.conversation_id, %{message | to: recipient_id})
  end

  # ─── Undelivered queue ─────────────────────────────────────────────────────

  def fetch_undelivered(user_id) do
    Logger.info("[DB] Fetching undelivered for #{user_id}")
    case Xandra.execute(MiniWa.Xandra,
           """
           SELECT message_id, conversation_id, sender_id, content, type
           FROM mini_wa.undelivered_messages
           WHERE recipient_id = ?
           """,
           [{"text", user_id}]) do
      {:ok, page} ->
        messages =
          page
          |> Enum.map(fn row ->
            type = row["type"] || "1:1"
            %{
              id:              row["message_id"],
              from:            row["sender_id"],
              to:              user_id,
              content:         row["content"],
              conversation_id: row["conversation_id"],
              type:            type,
              sent_at:         DateTime.utc_now() |> DateTime.to_iso8601()
            }
          end)
          |> Enum.sort_by(& &1.id)
        {:ok, messages}
      {:error, reason} -> {:error, reason}
    end
  end

  def mark_delivered(recipient_id, message_id) do
    case Xandra.execute(MiniWa.Xandra,
           "DELETE FROM mini_wa.undelivered_messages WHERE recipient_id = ? AND message_id = ?",
           [{"text", recipient_id}, {"text", message_id}]) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp insert_message(conv_id, message) do
    case Xandra.execute(MiniWa.Xandra,
           """
           INSERT INTO mini_wa.messages
             (conversation_id, message_id, sender_id, recipient_id, content, status, sent_at_ms)
           VALUES (?, ?, ?, ?, ?, 'sent', ?)
           """,
           [
             {"text",   conv_id},
             {"text",   message.id},
             {"text",   message.from},
             {"text",   message.to},
             {"text",   message.content},
             {"bigint", System.system_time(:millisecond)}
           ]) do
      {:ok, _} ->
        Logger.info("[DB] ✓ messages write | id=#{message.id}")
        :ok
      {:error, reason} ->
        Logger.error("[DB] ✗ messages write failed | #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp insert_undelivered(conv_id, message) do
    type = Map.get(message, :type, "1:1")
    case Xandra.execute(MiniWa.Xandra,
           """
           INSERT INTO mini_wa.undelivered_messages
             (recipient_id, message_id, conversation_id, sender_id, content, type)
           VALUES (?, ?, ?, ?, ?, ?)
           """,
           [
             {"text", message.to},
             {"text", message.id},
             {"text", conv_id},
             {"text", message.from},
             {"text", message.content},
             {"text", type}
           ]) do
      {:ok, _} -> :ok
      {:error, reason} ->
        Logger.error("[DB] ✗ undelivered write failed | #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp unix_ms_to_iso(nil), do: DateTime.utc_now() |> DateTime.to_iso8601()
  defp unix_ms_to_iso(ms),  do: DateTime.from_unix!(ms, :millisecond) |> DateTime.to_iso8601()
end
