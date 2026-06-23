defmodule MiniWa.Streaming.Consumer do
  @moduledoc """
  Kafka group consumer for the "messages" topic.

  For each message it writes to ScyllaDB (durable log) and, for offline
  recipients, adds an entry to the undelivered queue so drain_offline delivers
  it on reconnect.  Real-time delivery to online users is handled upstream:
  1:1 messages by the sender's Session (direct GenServer.cast), group messages
  by the sender's Session via Phoenix.PubSub broadcast to "group:<group_id>".
  """

  use GenServer
  require Logger

  @client :mini_wa_kafka
  @topic  "messages"
  @group  "mini_wa_consumer_group"

  # ─── Supervisor API ────────────────────────────────────────────────────────

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ─── GenServer callbacks ────────────────────────────────────────────────────

  @impl true
  def init(_) do
    schedule_start(3_000)
    {:ok, %{attempts: 0, subscriber_ref: nil}}
  end

  @impl true
  def handle_info(:start_subscriber, %{attempts: attempts} = state) do
    Logger.info("[Kafka][Consumer] Attempting to start subscriber (attempt #{attempts + 1})...")

    with :ok <- ensure_topic(),
         {:ok, pid} <- :brod_group_subscriber_v2.start_link(%{
           client:          @client,
           group_id:        @group,
           topics:          [@topic],
           cb_module:       __MODULE__,
           init_data:       [],
           message_type:    :message,
           consumer_config: [begin_offset: :latest]
         }) do
      ref = Process.monitor(pid)
      Logger.info("[Kafka][Consumer] ✓ Group subscriber running | group=#{@group} topic=#{@topic}")
      {:noreply, %{state | attempts: 0, subscriber_ref: ref}}
    else
      {:error, reason} ->
        delay = min(5_000 * (attempts + 1), 30_000)
        Logger.warning("[Kafka][Consumer] Not ready yet: #{inspect(reason)} — retrying in #{div(delay, 1000)}s")
        schedule_start(delay)
        {:noreply, %{state | attempts: attempts + 1}}
    end
  end

  # Subscriber died — restart it so delivery resumes automatically
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{subscriber_ref: ref} = state) do
    Logger.warning("[Kafka][Consumer] Subscriber died (#{inspect(reason)}) — restarting in 5s")
    schedule_start(5_000)
    {:noreply, %{state | subscriber_ref: nil}}
  end

  defp schedule_start(ms), do: Process.send_after(self(), :start_subscriber, ms)

  defp ensure_topic do
    brokers = Application.get_env(:mini_wa, MiniWa.Streaming, [])
              |> Keyword.get(:kafka_brokers, [{"localhost", 9092}])

    config = %{
      name:               @topic,
      num_partitions:     4,
      replication_factor: 1,
      assignments:        [],
      configs:            []
    }

    case :brod.create_topics(brokers, [config], %{timeout: 10_000}) do
      :ok ->
        Logger.info("[Kafka] ✓ Topic '#{@topic}' created")
        :ok

      {:error, [{:topic_already_exists, _}]} ->
        Logger.info("[Kafka] Topic '#{@topic}' already exists — OK")
        :ok

      {:error, reason} ->
        Logger.warning("[Kafka] create_topics: #{inspect(reason)}")
        :ok
    end
  end

  # ─── brod_group_subscriber_v2 callbacks ────────────────────────────────────
  # These are NOT GenServer callbacks — no @impl.

  # Called once per assigned partition.
  def init(%{group_id: gid, topic: t, partition: p}, init_data) do
    Logger.info("[Kafka][Consumer] Partition assigned | group=#{gid} topic=#{t} partition=#{p}")
    {:ok, init_data}
  end

  def handle_message({:kafka_message, _offset, _key, value, _ts_type, _ts, _headers}, state) do
    try do
      case Jason.decode(value) do
        {:ok, %{"type" => "group"} = payload} -> process_group(payload)
        {:ok, payload}                         -> process_1to1(payload)
        {:error, reason} -> Logger.error("[Kafka][Consumer] Decode failed: #{inspect(reason)}")
      end
    rescue
      e ->
        Logger.error("[Kafka][Consumer] ✗ handle_message crashed: #{Exception.message(e)}\n#{Exception.format_stacktrace(__STACKTRACE__)}")
    end

    {:ok, :commit, state}
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp build_message(payload) do
    %{
      id:                    payload["id"],
      type:                  payload["type"] || "1:1",
      from:                  payload["from"],
      to:                    payload["to"],
      content:               payload["content"],
      client_id:             payload["client_id"],
      sent_at:               payload["sent_at"],
      conversation_id:       payload["conversation_id"],
      client_sent_at:        payload["client_sent_at"],
      kafka_published_at_ms: payload["kafka_published_at_ms"],
      media_url:             payload["media_url"],
      media_type:            payload["media_type"]
    }
  end

  # 1:1: write to ScyllaDB + queue if offline.
  # Delivery to online recipients is handled by the sender's Session (direct cast).
  defp process_1to1(payload) do
    message = build_message(payload)

    Logger.info("[Kafka][Consumer] 1:1 persist | id=#{message.id} from=#{message.from} to=#{message.to}")

    recipient_online? = MiniWa.Cluster.online?(message.to)

    case MiniWa.DB.persist_message(message, recipient_online?) do
      :ok ->
        if recipient_online? do
          Logger.info("[Kafka][Consumer] #{message.to} online — Session already delivered, skipping queue")
        else
          Logger.info("[Kafka][Consumer] #{message.to} offline — queued in ScyllaDB")
        end
      {:error, reason} ->
        Logger.error("[Kafka][Consumer] ✗ ScyllaDB write failed | #{inspect(reason)}")
    end
  end

  # Group: write once to ScyllaDB, then queue only for offline members.
  # Online delivery is handled immediately by the sender's Session via PubSub —
  # the consumer only ensures offline members get the message when they reconnect.
  defp process_group(payload) do
    message  = build_message(payload)
    group_id = message.conversation_id

    Logger.info("[Kafka][Consumer] group persist | id=#{message.id} group=#{group_id}")

    with :ok <- MiniWa.DB.persist_group_message(message),
         {:ok, members} <- MiniWa.DB.list_group_members(group_id) do

      offline = Enum.reject(members, fn %{user_id: uid} ->
        uid == message.from || MiniWa.Cluster.online?(uid)
      end)

      Logger.info("[Kafka][Consumer] queuing for #{length(offline)} offline member(s) | id=#{message.id}")
      Enum.each(offline, fn %{user_id: uid} ->
        MiniWa.DB.queue_undelivered_for_member(uid, message)
      end)
    else
      {:error, reason} ->
        Logger.error("[Kafka][Consumer] ✗ group processing failed | #{inspect(reason)}")
      other ->
        Logger.error("[Kafka][Consumer] ✗ group processing unexpected return: #{inspect(other)}")
    end
  end
end
