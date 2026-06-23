defmodule MiniWa.Analytics.Consumer do
  @moduledoc """
  A second Kafka consumer group (mini_wa_analytics_group) that reads the same
  "messages" topic independently of the delivery consumer.  It only records
  metrics — no delivery, no ScyllaDB writes.  Runs entirely off the hot path.
  """

  use GenServer
  require Logger

  @client :mini_wa_kafka
  @topic  "messages"
  @group  "mini_wa_analytics_group"

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  # ─── GenServer callbacks ────────────────────────────────────────────────────

  @impl true
  def init(_) do
    # Small delay so the main consumer starts (and creates the topic) first.
    schedule_start(6_000)
    {:ok, %{attempts: 0}}
  end

  @impl true
  def handle_info(:start_subscriber, %{attempts: attempts} = state) do
    Logger.info("[Analytics][Consumer] Starting (attempt #{attempts + 1})...")

    case :brod_group_subscriber_v2.start_link(%{
      client:          @client,
      group_id:        @group,
      topics:          [@topic],
      cb_module:       __MODULE__,
      init_data:       [],
      message_type:    :message,
      consumer_config: [begin_offset: :latest]
    }) do
      {:ok, _pid} ->
        Logger.info("[Analytics][Consumer] ✓ Running | group=#{@group}")
        {:noreply, %{state | attempts: 0}}

      {:error, reason} ->
        delay = min(5_000 * (attempts + 1), 30_000)
        Logger.warning("[Analytics][Consumer] Not ready: #{inspect(reason)} — retrying in #{div(delay, 1000)}s")
        schedule_start(delay)
        {:noreply, %{state | attempts: attempts + 1}}
    end
  end

  defp schedule_start(ms), do: Process.send_after(self(), :start_subscriber, ms)

  # ─── brod_group_subscriber_v2 callbacks ────────────────────────────────────
  # Arity and pattern distinguish these from the GenServer init/1 above.

  def init(%{group_id: gid, partition: p}, init_data) do
    Logger.info("[Analytics][Consumer] Partition assigned | group=#{gid} partition=#{p}")
    {:ok, init_data}
  end

  def handle_message({:kafka_message, _offset, _key, value, _ts_type, _ts, _headers}, state) do
    case Jason.decode(value) do
      {:ok, payload} -> record_metrics(payload)
      {:error, _}    -> :ok
    end
    {:ok, :commit, state}
  end

  # ─── Private ───────────────────────────────────────────────────────────────

  defp record_metrics(payload) do
    MiniWa.Analytics.Store.record_message(payload["media_type"])

    client_sent_at        = payload["client_sent_at"]
    kafka_published_at_ms = payload["kafka_published_at_ms"]

    if is_integer(client_sent_at) && is_integer(kafka_published_at_ms) do
      MiniWa.Analytics.Store.record_latency(kafka_published_at_ms - client_sent_at)
    end
  end
end
