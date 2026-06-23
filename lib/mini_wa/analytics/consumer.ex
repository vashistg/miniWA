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
    {:ok, %{attempts: 0, subscriber_ref: nil}}
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
      {:ok, pid} ->
        ref = Process.monitor(pid)
        Logger.info("[Analytics][Consumer] ✓ Running | group=#{@group}")
        {:noreply, %{state | attempts: 0, subscriber_ref: ref}}

      {:error, reason} ->
        delay = min(5_000 * (attempts + 1), 30_000)
        Logger.warning("[Analytics][Consumer] Not ready: #{inspect(reason)} — retrying in #{div(delay, 1000)}s")
        schedule_start(delay)
        {:noreply, %{state | attempts: attempts + 1}}
    end
  end

  # Subscriber crashed — restart it
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{subscriber_ref: ref} = state) do
    Logger.warning("[Analytics][Consumer] Subscriber crashed (#{inspect(reason)}) — restarting in 5s")
    schedule_start(5_000)
    {:noreply, %{state | subscriber_ref: nil}}
  end

  defp schedule_start(ms), do: Process.send_after(self(), :start_subscriber, ms)

  # ─── brod_group_subscriber_v2 callbacks ────────────────────────────────────
  # Arity and pattern distinguish these from the GenServer init/1 above.

  def init(%{group_id: gid, partition: p}, init_data) do
    Logger.info("[Analytics][Consumer] Partition assigned | group=#{gid} partition=#{p}")
    {:ok, init_data}
  end

  # Metrics are now recorded directly in the sender's Session at publish time.
  # This consumer only commits offsets so Kafka lag stays current.
  def handle_message(_msg, state) do
    {:ok, :commit, state}
  end
end
